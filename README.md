# README

Tooling for the Ceph team

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

