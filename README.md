# README

Tooling for the Ceph team

## Packaging

To build a standalone PEX installer that bundles the CLI and its dependencies:

```
just build-pex
./dist/cephtools.pex --help
```

The resulting archive is written to `dist/cephtools.pex`. 

## VMaaS bootstrap (`cephtools vmaas`)

Use `cephtools vmaas` to stand up or manage a local MAAS/LXD/Juju lab for VMaaS development. 

- `cephtools vmaas install`: runs the full workflow: install snaps, initialise LXD, bring up MAAS, register the LXD VM host, configure networking, bootstrap Juju, and ensure the default model exists.

Below are the individual steps:

- `cephtools vmaas install-deps`: installs the required snaps (`maas`, `maas-test-db`, `lxd`, `terraform`) and Terragrunt, then checks LXD is ready.
- `cephtools vmaas lxd-init`:runs the non-interactive LXD initialisation using the configured bridge.
- `cephtools vmaas maas-init`:initialises MAAS (region + rack), creates/logs in the admin user, and writes `cloud.yaml`.
- `cephtools vmaas register-vm-host` registers the local LXD as a VM host in MAAS and kicks off boot-resource imports.
- `cephtools vmaas configure-network` : configures the default VLAN in MAAS (gateway, DHCP range, space) and records the details in `~/.local/state/cephtools/network.yaml`.
- `cephtools vmaas ensure-nodes`: reconciles the VM inventory via Terragrunt; override the number of VMs and attached data disks with `--vm-count`, `--vm-data-disk-count`, and `--vm-data-disk-size`.
- `cephtools vmaas juju-init`: verifies MAAS/LXD health, installs Juju, writes credentials, onboards the cloud, and bootstraps the controller.

Set `CEPHTOOLS_TERRAGRUNT_DIR` or the `terragrunt_dir` key in `cephtools.yaml` to point at your Terragrunt plans if they live outside the repository. The MicroCeph Terragrunt/Terraform module now lives in the
[charm-microceph](https://github.com/canonical/charm-microceph/tree/main/terraform/microceph) repository.

## MicroCeph helpers (`cephtools microceph`)

Utilities that execute MicroCeph management commands across every unit in a deployment. Node discovery defaults to the `microceph` application machines in the model configured by `juju_model`; pass `--nodes` to override.

`cephtools microceph disk add <args>`: runs `microceph disk add ...` on each node. Combine with:
- `--nodes <machine-id>` (repeatable) to target specific Juju machine IDs, defaults to all.
- `--dry-run` to print the commands without executing them.

Use `--` to pass in args to the invoked `microceph disk add` command.


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
