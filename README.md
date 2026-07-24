# README

Tooling for the Ceph team

## Install


### Binary

Install the PXE based binary:

```
sudo wget -O /usr/local/bin/cephtools https://github.com/canonical/cephtools/releases/download/latest/cephtools
sudo chmod +x /usr/local/bin/cephtools

```

### Source

Clone source and install via uv:

```
sudo snap install astral-uv --classic
git clone https://github.com/canonical/cephtools.git
cd cephtools
uv pip install --system --prefix ~/.local .
export PATH="$PATH:$HOME/.local/bin"
```

## Packaging

To build a standalone PEX installer that bundles the CLI and its dependencies:

```
just build-pex
./dist/cephtools.pex --help
```

The resulting archive is written to `dist/cephtools.pex`. 

## Testflinger deployment (`cephtools testflinger`)

Commands to reserve nodes via Testflinger and bootstrap testing environments.

### Reserve a node

Reserve a machine for interactive use:

```bash
cephtools testflinger reserve [QUEUE_NAME] \
    --launchpad-account <ssh-key-ref> \
    --reserve-for <seconds>
```

`--launchpad-account` must be a Testflinger SSH key reference, for example `lp:<launchpad-id>` or `gh:<github-username>`.

This submits a job to the specified queue (defaults to `ceph-qa-1`), waits for the reservation to become active, and prints the SSH connection details.

### Deploy a test environment

Reserve a machine and automatically deploy `cephtools` and its test environment dependencies:

```bash
cephtools testflinger deploy [QUEUE_NAME] \
    --launchpad-account <ssh-key-ref> \
    --reserve-for <seconds> \
    --testenv-args "--substrate maas-vm"
```

This command:
1. Reserves a node on the specified queue.
2. SSHs into the node once active.
3. Installs `cephtools` and dependencies (LXD, MAAS, Juju, etc.) via `cephtools testenv install`.

Set `--testenv-args` (or `CEPHTOOLS_TESTENV_ARGS`) to pass additional arguments to the remote `cephtools testenv ... install` command.

Once complete, it provides the SSH command to access the ready-to-use test environment.

### Cancel a reservation

Cancel an active Testflinger reservation by job id:

```bash
cephtools testflinger cancel <job-id>
```

Or cancel the latest reservation previously created by `cephtools testflinger reserve` or `cephtools testflinger deploy`:

```bash
cephtools testflinger cancel --latest
```

This is a thin wrapper around `testflinger cancel <job-id>`.


## Testenv bootstrap (`cephtools testenv`)

Use `cephtools testenv` to stand up or manage a local MAAS/LXD/Juju lab for test environment development.

The substrate is selected with `--substrate`:

- `lxd` (default): skip MAAS entirely, bootstrap Juju on the local LXD cloud, create an `ext` LXD network, expose it as the Juju `external` space, and provision OSD block devices as LXD custom block volumes attached to Juju LXD VMs.
- `maas-host`: install MAAS on the host and use MAAS to compose LXD VMs.
- `maas-vm`: run MAAS inside an isolated LXD VM and use MAAS to compose LXD VMs.

Testenv uses fixed disposable-lab conventions for resource names, bridges, MAAS credentials, and the `cephtools` Juju model. Meaningful test variations are CLI-only: `--substrate`, `--maas-version`, `--maas-vm-cpus`, `--maas-vm-memory`, `--maas-vm-disk`, and `--maas-vm-image`. There is no general `cephtools.yaml`; legacy files with that name are ignored.

- `cephtools testenv install`: runs the workflow for the selected substrate and ensures the default Juju model exists.

Below are the individual steps:

- `cephtools testenv install-deps`: installs the substrate dependencies. Every substrate installs LXD and Terraform; the LXD substrate also installs Juju, while MAAS substrates install MAAS/Terragrunt as needed.
- `cephtools testenv lxd-init`: runs non-interactive LXD initialisation. In LXD substrate mode it creates both the normal bridge and the `ext` bridge with LXD-managed DHCP/DNS.
- `cephtools testenv maas-init`: MAAS-only; configures MAAS and writes MAAS cloud state.
- `cephtools testenv register-vm-host`: MAAS-only; registers local LXD as a MAAS VM host and kicks off boot-resource imports.
- `cephtools testenv configure-network`: configures MAAS VLANs/spaces on MAAS substrates. In LXD substrate mode it runs `juju reload-spaces`, creates/moves the `external` space for `ext`, and records reduced network details in `state/network.yaml`.
- `cephtools testenv ensure-nodes`: reconciles the VM inventory. MAAS substrates use Terragrunt/MAAS; the LXD substrate adds Juju LXD VM machines and attaches LXD custom block volumes for OSD disks. Override with `--vm-count`, `--vm-data-disk-count`, and `--vm-data-disk-size`.
- `cephtools testenv cleanup`: best-effort cleanup for testenv-managed lab resources and generated state. On MAAS substrates it destroys Terragrunt-managed nodes, kills the Juju controller, removes the fixed MAAS VM host, deletes known transient LXD instances such as `warmup-vm`, and removes generated state files (`cloud.yaml`, `cred.yaml`, `network.yaml`). On the LXD substrate it kills `lxd-controller`, removes generated state, and deletes matching LXD OSD block volumes. `--keep-nodes` also preserves the Juju controller on every substrate; this is required for LXD nodes because they cannot survive controller removal. By default the command preserves the installed toolchain; add `--purge-installed` for maximum isolation.
- `cephtools testenv juju-init`: verifies LXD/MAAS health as applicable, installs Juju, and bootstraps the selected substrate controller (`maas-controller` or `lxd-controller`).

Examples:

```bash
# Reclaim all testenv-managed lab resources and generated state
cephtools testenv cleanup

# Preview the cleanup plan without making changes
cephtools testenv cleanup --dry-run

# Keep the Juju controller and generated state, but clean the rest
cephtools testenv cleanup --keep-controller --keep-state

# Maximum-isolation cleanup: also purge the installed testenv toolchain
cephtools testenv cleanup --purge-installed
```

`cleanup` is idempotent and best effort: if a phase has nothing to remove it is reported as skipped, later phases still run after an earlier failure, and the command exits non-zero only after printing the final summary when at least one phase failed. `--purge-installed` is intentionally destructive and cannot be combined with `--keep-*` preservation flags.

### Reconnectable test jobs

`cephtools testenv job` runs a long command on a prepared remote test environment without tying its lifetime to SSH. The controller and host must use releases with the same protocol, reported by `cephtools testenv job protocol`.

A CI job uses three lifecycle commands:

```bash
cephtools testenv job start \
  --target ubuntu@HOST --run-id RUN_ID \
  --run-root /home/ubuntu/cephtools-runs \
  --lock-file /run/lock/cephtools-testenv-job.lock \
  --runtime-seconds 7200 --stop-timeout-seconds 300 \
  --stage ./payload.sh payload.sh \
  -- /home/ubuntu/cephtools-runs/RUN_ID/payload.sh

cephtools testenv job wait \
  --target ubuntu@HOST --run-id RUN_ID \
  --run-root /home/ubuntu/cephtools-runs

# Run from a separate CI `always()` finalizer.
cephtools testenv job stop \
  --target ubuntu@HOST --run-id RUN_ID \
  --run-root /home/ubuntu/cephtools-runs
```

`start` checks the remote protocol, transactionally stages immutable files into an exclusively claimed run directory, refuses a busy shared host, and launches a bounded, collectable transient systemd unit. A malformed or interrupted staging transfer leaves no final run directory. The host agent holds the nonblocking lock for the payload's complete lifetime and writes output directly to `run.log`. It atomically writes `status.json`; a final `finished` or `terminated` status with a numeric exit code is authoritative even after systemd garbage-collects the transient unit. `wait` tolerates bounded SSH transport interruptions, validates every status identity, fails immediately on a definitive remote lifecycle error, and returns the payload exit code. `stop` targets only the deterministic unit for the validated run ID and is idempotent for an authoritative final state or a unit that clearly never launched. If durable status is malformed, it still performs the exact-unit stop and reports a structured best-effort outcome instead of failing the finalizer.

`--stop-timeout-seconds` must be at least 10 seconds. The host agent reserves five seconds of that timeout for writing final durable status; the remainder is available to the payload process group for TERM-triggered cleanup before SIGKILL.

Keep finalization in a separate CI step so it can run when waiting fails or is cancelled. `RuntimeMaxSec` remains the independent backstop if the runner cannot reconnect. Product-specific setup, cleanup, and artifact upload should remain outside the generic job lifecycle.

Set `CEPHTOOLS_TERRAGRUNT_DIR` or `CEPHTOOLS_TERRAFORM_ROOT` to point at plans outside the repository. The MicroCeph Terragrunt/Terraform module now lives in the
[charm-microceph](https://github.com/canonical/charm-microceph/tree/main/terraform/microceph) repository.

## MicroCeph helpers (`cephtools microceph`)

Utilities that execute MicroCeph management commands across every unit in a deployment. Node discovery defaults to the `microceph` application machines in the `cephtools` model; pass `--model` or `--nodes` to override.

`cephtools microceph disk add <args>`: runs `microceph disk add ...` on each node. Combine with:
- `--model <model>` to discover units in a model other than `cephtools`.
- `--nodes <machine-id>` (repeatable) to target specific Juju machine IDs directly.
- `--dry-run` to print the commands without executing them.

Use `--` to pass in args to the invoked `microceph disk add` command, for instance:

```
cephtools microceph disk add -- --all-available
```



## Release tooling

### Getting charm PRs

```
$ cephtools list-prs --help                                                                        
Usage: cephtools list-prs [OPTIONS] CHARM SOURCE TARGET BASE BASE_BRANCH

  A tool to list PRs for a given charm between releases.

Options:
  --repo TEXT  Path to the git repository for the charms.
  --help       Show this message and exit.

```

Used to determine which PRs have been closed between different channels

Example:

```
cephtools list-prs --repo ~/src/ceph-charms ceph-mon squid/candidate squid/edge ubuntu@24.04 main
#105  [DNM] Caracal verification
https://github.com/canonical/ceph-charms/pull/105  closedAt: 2025-08-12T15:33:42Z

#104  [DNM] Caracal verification
https://github.com/canonical/ceph-charms/pull/104  closedAt: 2025-08-04T22:17:32Z

#89  [DNM] Run tests with 19.2.1 noble PPA
https://github.com/canonical/ceph-charms/pull/89  closedAt: 2025-08-06T15:37:46Z
```


### Releasing charms

```
$ cephtools charm-rel --help
Usage: cephtools charm-rel [OPTIONS] SOURCE TARGET BASE [CHARMS]...

  Release charm revisions from a source channel to a target channel.

Options:
  --apply / --no-apply  Apply the release. If not present, a dry-run is
                        performed.
  --help                Show this message and exit.

```

Used to release charms from one channel to another. Does a dry-run by default.

Example invocation:

```
$ cephtools charm-rel quincy/candidate quincy/stable 22.04  ceph-dashboard  ceph-fs  ceph-iscsi  ceph-mon  ceph-nfs  ceph-osd  ceph-proxy  ceph-radosgw  ceph-rbd-mirror
...
```
