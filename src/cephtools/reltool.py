import click

import subprocess
import tempfile
import zipfile
import json
import os
from datetime import datetime


def download_and_get_ts(charm, channel, base, verbose=False):
    # ensure juju’s common snap tmp dir exists
    juju_tmp = os.path.expanduser("~/snap/juju/common")
    os.makedirs(juju_tmp, exist_ok=True)

    # download charm into that dir, silently
    dest = tempfile.NamedTemporaryFile(suffix=".charm", delete=False, dir=juju_tmp).name
    if verbose:
        click.echo(f"Downloading {charm} from channel {channel} (base {base}) to {dest}")
    subprocess.check_call(
        [
            "juju",
            "download",
            charm,
            "--channel",
            channel,
            "--base",
            base,
            "--filepath",
            dest,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # extract git-info.txt and then delete the file
    try:
        with zipfile.ZipFile(dest) as z:
            with z.open("git-info.txt") as f:
                for line in f:
                    line = line.decode()
                    if line.startswith("commit_date:"):
                        ts = line.split(":", 1)[1].strip()
                        parsed = datetime.fromisoformat(ts)
                        if verbose:
                            click.echo(f"  commit_date for {charm} ({channel}): {parsed}")
                        return parsed
        raise RuntimeError("commit_date not found in git-info.txt")
    finally:
        try:
            os.remove(dest)
        except OSError:
            pass


def run_gh_pr_list(gh_base, repo_path):
    proc = subprocess.run(
        [
            "gh",
            "pr",
            "list",
            "--base",
            gh_base,
            "--state",
            "closed",
            "--json",
            "number,url,closedAt,title,files",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=repo_path,
    )
    return json.loads(proc.stdout)


def run_charmcraft_status(charm):
    """Run charmcraft status and return the JSON output."""
    proc = subprocess.run(
        ["charmcraft", "status", charm, "--format", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(proc.stdout)


def get_prs(gh_base, charm_name, start_ts, end_ts, repo_path, verbose=False):
    prs = run_gh_pr_list(gh_base, repo_path)
    if verbose:
        click.echo(f"Found {len(prs)} closed PRs on base {gh_base}")
        click.echo(f"Filtering PRs between {start_ts} and {end_ts} touching {charm_name}/")

    def parse(ts):
        return datetime.fromisoformat(ts)

    matched = []
    for pr in prs:
        closed = parse(pr["closedAt"])
        if start_ts < closed <= end_ts:
            # include only if any file path under the charm subdir
            for f in pr.get("files", []):
                if f.get("path", "").startswith(f"{charm_name}/"):
                    matched.append(pr)
                    break
    return matched


@click.command()
@click.argument("charm")
@click.argument("source")
@click.argument("target")
@click.argument("base")
@click.argument("base_branch")
@click.option("--repo", default=".", help="Path to the git repository for the charms.")
@click.option("--verbose", is_flag=True, default=False, help="Print diagnostic information.")
def list_prs(charm, source, target, base, base_branch, repo, verbose):
    """A tool to list PRs for a given charm between releases."""
    src_ts = download_and_get_ts(charm, source, base, verbose=verbose)
    tgt_ts = download_and_get_ts(charm, target, base, verbose=verbose)

    matched = get_prs(base_branch, charm, src_ts, tgt_ts, repo, verbose=verbose)
    for pr in matched:
        print(
            f"#{pr['number']}  {pr['title']}\n{pr['url']}  closedAt: {pr['closedAt']}\n"
        )


@click.command(name="charm-rel")
@click.argument("source")
@click.argument("target")
@click.argument("base")
@click.argument("charms", nargs=-1)
@click.option(
    "--apply/--no-apply",
    default=False,
    help="Apply the release. If not present, a dry-run is performed.",
)
@click.option("--verbose", is_flag=True, default=False, help="Print diagnostic information.")
def charm_rel(source, target, base, charms, apply, verbose):
    """Release charm revisions from a source channel to a target channel."""
    for charm in charms:
        print(f"\n--- {charm} ---")
        if not apply:
            print("Dry run mode: no changes will be made.")

        try:
            if verbose:
                click.echo(f"Fetching charmcraft status for {charm}...")
            status_data = run_charmcraft_status(charm)
            if verbose:
                click.echo(f"  Got {len(status_data)} track entries")
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            print(f"Could not get status for charm {charm}: {e}")
            continue

        revisions = []
        for entry in status_data:
            for mapping in entry.get("mappings", []):
                base_info = mapping.get("base")
                if not base_info:
                    continue
                base_channel = base_info.get("channel")
                if verbose:
                    click.echo(f"  base channel={base_channel!r} (want {base!r})")
                if base_channel == base:
                    for release in mapping.get("releases", []):
                        rel_channel = release.get("channel")
                        if verbose:
                            click.echo(f"    release channel={rel_channel!r} rev={release.get('revision')} (want {source!r})")
                        if rel_channel == source:
                            revisions.append(str(release["revision"]))

        if verbose:
            click.echo(f"  Found revisions for base {base}, source {source}: {revisions}")

        for revision in revisions:
            if apply:
                print(f"Releasing {charm} {revision} to {target}...")
                try:
                    subprocess.run(
                        ["charmcraft", "release", "-r", revision, "-c", target, charm],
                        check=True,
                    )
                except subprocess.CalledProcessError as e:
                    print(f"Failed to release charm {charm} revision {revision}: {e}")
            else:
                print(f"  would release {charm} {revision} to {target}")
