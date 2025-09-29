import click

import subprocess
import tempfile
import zipfile
import json
import os
from datetime import datetime


def download_and_get_ts(charm, channel, base):
    # ensure juju’s common snap tmp dir exists
    juju_tmp = os.path.expanduser("~/snap/juju/common")
    os.makedirs(juju_tmp, exist_ok=True)

    # download charm into that dir, silently
    dest = tempfile.NamedTemporaryFile(suffix=".charm", delete=False, dir=juju_tmp).name
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
                        return datetime.fromisoformat(ts)
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


def get_prs(gh_base, charm_name, start_ts, end_ts, repo_path):
    prs = run_gh_pr_list(gh_base, repo_path)

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
def list_prs(charm, source, target, base, base_branch, repo):
    """A tool to list PRs for a given charm between releases."""
    src_ts = download_and_get_ts(charm, source, base)
    tgt_ts = download_and_get_ts(charm, target, base)

    matched = get_prs(base_branch, charm, src_ts, tgt_ts, repo)
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
def charm_rel(source, target, base, charms, apply):
    """Release charm revisions from a source channel to a target channel."""
    for charm in charms:
        print(f"\n--- {charm} ---")
        if not apply:
            print("Dry run mode: no changes will be made.")

        try:
            status_data = run_charmcraft_status(charm)
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            print(f"Could not get status for charm {charm}: {e}")
            continue

        revisions = []
        for entry in status_data:
            for mapping in entry.get("mappings", []):
                base_info = mapping.get("base")
                if not base_info:
                    continue
                if base_info.get("channel") == base:
                    for release in mapping.get("releases", []):
                        if release.get("channel") == source:
                            revisions.append(str(release["revision"]))

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
