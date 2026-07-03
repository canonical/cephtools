# Isolate MAAS in an LXD VM

## Goal

Change the `cephtools testenv` architecture so that MAAS runs inside a dedicated LXD VM instead of directly on the host. The host LXD daemon remains available for normal use, while MAAS controls a separate LXD-backed lab network and can compose LXD VMs through the host LXD API.

This should reduce DNS/DHCP conflicts between host LXD and MAAS by ensuring they never manage the same subnet.

## Proposed topology

```text
Host
├── LXD daemon
├── host LXD network, e.g. lxdbr0
│   ├── managed by LXD
│   ├── LXD DHCP enabled
│   └── LXD DNS enabled
│
├── MAAS LXD network, e.g. maasbr0
│   ├── managed by LXD bridge/NAT
│   ├── LXD DHCP disabled
│   ├── LXD DNS disabled
│   └── DHCP/DNS served by MAAS rackd inside the MAAS VM
│
└── MAAS VM
    ├── static IP on maasbr0
    ├── MAAS region+rack services
    ├── PostgreSQL / MAAS dependencies
    └── registered client for host LXD at https://<host-maasbr0-ip>:8443
```

## Design principles

1. **Strict network ownership**
   - LXD owns the normal host network, such as `lxdbr0`.
   - MAAS owns a separate network, such as `maasbr0`.
   - Never enable LXD DHCP/DNS and MAAS DHCP/DNS on the same subnet.

2. **Host LXD remains functional**
   - Existing host LXD usage should continue to work through the default profile and default managed network.
   - MAAS-created machines should use the MAAS-owned network.

3. **MAAS VM is a bootstrap/control instance**
   - The MAAS VM is created by `cephtools`, not by MAAS.
   - It should not be enlisted, commissioned, or managed as a MAAS machine.
   - Cleanup should treat it specially.

4. **MAAS controls lab DNS/DHCP only**
   - MAAS bind9/rackd should serve the MAAS lab network from inside the VM.
   - Host-level bind9 should not be installed or configured for testenv.
   - Host-level LXD dnsmasq should only serve the host LXD network.

## New configuration

Add testenv defaults for the isolated mode.

Suggested config keys:

```yaml
testenv:
  maas_mode: vm              # existing/direct-host vs vm
  lxdbridge: lxdbr0          # normal host LXD network
  maas_lxdbridge: maasbr0    # MAAS-owned network
  maas_vm_name: maas-vm
  maas_vm_cpus: 8
  maas_vm_memory: 16GiB
  maas_vm_disk: 80GiB
  maas_vm_ip: null           # optional; derive from maasbr0 if unset
  maas_vm_image: ubuntu:24.04
  vmhost_name: local-lxd
```

Potentially keep the existing host-installed MAAS path as `maas_mode: host` until the VM mode is stable.

## Implementation plan

### 1. Split LXD network setup into two explicit roles

Current code creates LXD networks with DNS/DHCP disabled because MAAS is installed on the host and must own the bridge.

Refactor this into separate helpers:

- `ensure_lxd_host_network(name)`
  - creates or verifies the normal host LXD network;
  - keeps LXD DHCP/DNS enabled;
  - attaches the default profile to this network.

- `ensure_lxd_maas_network(name, ipv4_address=None)`
  - creates or verifies the MAAS lab network;
  - enables NAT if desired;
  - disables LXD DHCP;
  - disables LXD DNS;
  - does **not** attach the default profile unless explicitly requested.

The current `ensure_lxd_network(... dns.mode=none ipv4.dhcp=false ...)` behavior should become the MAAS-network helper, not the default behavior for all LXD networks.

### 2. Initialize host LXD without stopping host bind9

In VM mode, host MAAS/bind9 should not exist. LXD can be initialized normally.

Changes:

- keep `_run_lxd_minimal_init()`;
- configure `core.https_address=:8443`;
- configure authentication/trust for MAAS to access host LXD;
- create both networks:
  - `lxdbr0`: LXD-owned, DNS/DHCP enabled;
  - `maasbr0`: MAAS-owned, DNS/DHCP disabled;
- avoid stopping/starting host bind9 in VM mode.

### 3. Create the MAAS VM

Add a new step, for example:

```text
cephtools testenv maas-vm-init
```

Responsibilities:

1. launch a VM:

   ```bash
   lxc launch ubuntu:24.04 maas-vm --vm \
     -c limits.cpu=8 \
     -c limits.memory=16GiB \
     -d root,size=80GiB
   ```

2. attach it to the MAAS network:

   ```bash
   lxc config device add maas-vm eth0 nic network=maasbr0 name=eth0
   ```

3. assign or discover a stable MAAS VM IP;
4. wait for cloud-init/SSH;
5. install `cephtools` or run a rendered bootstrap script inside the VM.

Prefer a stable IP on `maasbr0`. Options:

- configure the VM with cloud-init/netplan static addressing;
- reserve an address outside the MAAS dynamic range;
- use LXD NIC `ipv4.address` if compatible with the chosen network setup.

### 4. Install MAAS inside the VM

Reuse as much of the existing MAAS setup logic as possible, but execute it inside the MAAS VM.

Possible approaches:

- use `lxc exec maas-vm -- ...` wrappers for commands;
- upload a generated bootstrap script and run it inside the VM;
- install `cephtools` inside the VM and invoke a VM-local subset of `cephtools testenv`.

Initial pragmatic path:

1. upload a bootstrap script to the VM;
2. install package dependencies inside the VM;
3. configure PostgreSQL-backed MAAS;
4. create the MAAS admin user;
5. run `maas login` inside the VM;
6. export the API key back to the host state directory.

Avoid installing MAAS packages, PostgreSQL, or bind9 on the host in VM mode.

### 5. Configure MAAS networking for `maasbr0`

The host should derive:

- `maasbr0` CIDR;
- host-side gateway IP;
- MAAS VM static IP;
- planned MAAS dynamic DHCP range.

Inside MAAS:

1. ensure the subnet exists;
2. set gateway to the host-side `maasbr0` IP;
3. create a dynamic IP range excluding:
   - host gateway;
   - MAAS VM static IP;
   - any reserved infrastructure addresses;
4. enable DHCP on the VLAN using the MAAS rack controller inside the VM;
5. create/assign spaces:
   - existing `jujuspace` for Juju bootstrap;
   - optionally `external` if still needed.

### 6. Register host LXD as a MAAS VM host

From the MAAS VM, the host LXD API should be reachable at:

```text
https://<host-maasbr0-ip>:8443
```

Update `register_lxd_vmhost_impl` to support a remote MAAS CLI execution context or add a VM-mode wrapper that runs:

```bash
maas admin vm-hosts create \
  type=lxd \
  name=local-lxd \
  project=default \
  power_address=https://<host-maasbr0-ip>:8443 \
  password=<trust-password-or-token>
```

Consider moving away from `core.trust_password` if MAAS supports certificate/trust-token based registration for the target MAAS version.

### 7. Ensure MAAS-composed VMs attach to the MAAS-owned network

This is the riskiest part and should be validated early.

Questions to answer experimentally:

- When MAAS composes a VM on a registered LXD host, which LXD network/profile does it attach to?
- Can MAAS choose the LXD network/project during composition?
- Does the MAAS LXD VM host registration expose network selection?
- Do we need a dedicated LXD project for MAAS with a project default profile using `maasbr0`?

Likely preferred approach:

- create an LXD project for MAAS-managed VMs, for example `maas`;
- configure that project's default profile to attach NICs to `maasbr0`;
- register the host LXD VM host with `project=maas` instead of `project=default`.

This keeps normal host LXD instances in the default project/network and MAAS instances in the MAAS project/network.

### 8. Update Juju onboarding

The host writes `cloud.yaml` and `cred.yaml` using the MAAS VM API endpoint:

```yaml
clouds:
  maas-cloud:
    type: maas
    auth-types: [oauth1]
    endpoint: http://<maas-vm-ip>:5240/MAAS
```

Then Juju bootstrap should work as before, using MAAS to compose machines through host LXD.

Validate that the host can reach:

- MAAS API on the MAAS VM;
- MAAS DNS if needed;
- Juju controller machines created by MAAS on `maasbr0`.

### 9. Cleanup behavior

Extend cleanup to understand VM mode.

Default cleanup should remove:

- Juju controller/model resources;
- MAAS-composed machines;
- MAAS LXD VM host registration;
- MAAS VM, unless `--keep-maas-vm` is provided;
- MAAS-specific LXD project/profile if empty;
- generated state files.

It should preserve by default:

- host LXD installation;
- normal host LXD network;
- unrelated LXD instances/projects.

`--purge-installed` in VM mode should not remove host MAAS/PostgreSQL because they should not be installed. It may optionally delete the MAAS VM and MAAS lab network.

### 10. Testing plan

Use a fresh Testflinger node or fresh VM for each full integration test.

#### Unit tests

Add tests for:

- VM-mode config defaults;
- host network creation leaves DNS/DHCP enabled;
- MAAS network creation disables DNS/DHCP;
- generated MAAS VM bootstrap script;
- generated cloud/credential/network state files;
- VM-mode cleanup plan;
- VM-mode command sequencing.

#### Integration tests

Run on a fresh machine:

1. `cephtools testenv install --maas-mode vm`;
2. verify host LXD default network resolves normal LXD instances;
3. verify MAAS VM services are active;
4. verify MAAS DHCP/DNS is active on `maasbr0`;
5. verify host LXD VM host appears in MAAS;
6. compose one MAAS machine;
7. bootstrap Juju;
8. deploy a small charm or simple workload;
9. run cleanup;
10. verify host LXD remains usable.

#### DNS-specific validation

Check listeners:

```bash
sudo ss -lntup | grep ':53'
lxc network show lxdbr0
lxc network show maasbr0
```

Expected:

- LXD dnsmasq listens for the host-owned LXD network;
- MAAS bind9/rackd listen inside the MAAS VM;
- no host-level bind9 conflict;
- no LXD dnsmasq on the MAAS-owned bridge.

## Migration strategy

1. Add VM-mode code paths behind a config flag/CLI option.
2. Keep existing host-mode behavior as the default initially.
3. Add unit tests for both modes.
4. Validate VM mode manually on fresh Testflinger nodes.
5. Switch default to VM mode only after repeated successful end-to-end runs.
6. Eventually deprecate host-mode if VM mode proves reliable.

## Open questions

- Can MAAS reliably compose LXD VMs into a non-default LXD project?
- Does MAAS require password auth for LXD, or can we use trust tokens/certificates?
- Should the MAAS VM have one NIC on `maasbr0`, or two NICs: one host/LXD-managed NIC for API access and one MAAS-owned NIC for DHCP/DNS?
- Should `maasbr0` use NAT through the host, or should routing be made explicit?
- How should host DNS resolution of MAAS-managed names be handled, if needed?
- Can the existing Terragrunt MAAS node reconciliation continue unchanged, or does it need to know about the LXD project/network?

## Suggested first spike

Before large refactoring, manually validate the key risk:

1. Create `maasbr0` with LXD DHCP/DNS disabled.
2. Launch a MAAS VM on `maasbr0` with a static IP.
3. Install MAAS inside the VM.
4. Register host LXD from the MAAS VM using `project=maas`.
5. Compose one VM through MAAS.
6. Confirm the composed VM lands on `maasbr0`, receives MAAS DHCP, resolves MAAS DNS, and can reach the archive.

If this works, the rest of the change is mostly orchestration and cleanup work.
