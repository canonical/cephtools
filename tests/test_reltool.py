import json
import subprocess
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest
from click.testing import CliRunner

from cephtools.reltool import (
    charm_rel,
    download_and_get_ts,
    get_prs,
    run_charmcraft_status,
)


@patch("cephtools.reltool.run_gh_pr_list")
def test_get_prs(mock_run_gh_pr_list):
    """Verify that get_prs filters PRs correctly."""

    charm_name = "my-charm"
    start_ts = datetime(2025, 7, 1, tzinfo=timezone.utc)
    end_ts = datetime(2025, 7, 31, tzinfo=timezone.utc)

    mock_prs_data = [
        # 1. Should match: closed in range, file path matches charm
        {
            "number": 1,
            "title": "Good PR",
            "url": "url1",
            "closedAt": "2025-07-15T10:00:00Z",
            "files": [{"path": "my-charm/src/charm.py"}],
        },
        # 2. Too early
        {
            "number": 2,
            "title": "Too early",
            "url": "url2",
            "closedAt": "2025-06-30T10:00:00Z",
            "files": [{"path": "my-charm/src/charm.py"}],
        },
        # 3. Too late
        {
            "number": 3,
            "title": "Too late",
            "url": "url3",
            "closedAt": "2025-08-01T10:00:00Z",
            "files": [{"path": "my-charm/src/charm.py"}],
        },
        # 4. Wrong path
        {
            "number": 4,
            "title": "Wrong path",
            "url": "url4",
            "closedAt": "2025-07-15T11:00:00Z",
            "files": [{"path": "other-charm/src/charm.py"}],
        },
        # 5. No files
        {
            "number": 5,
            "title": "No files",
            "url": "url5",
            "closedAt": "2025-07-15T12:00:00Z",
            "files": [],
        },
        # 6. Another good one, to ensure we get multiple
        {
            "number": 6,
            "title": "Another Good PR",
            "url": "url6",
            "closedAt": "2025-07-20T10:00:00Z",
            "files": [{"path": "my-charm/hooks/install"}, {"path": "unrelated/file"}],
        },
        # 7. On the boundary (end_ts) - should be included
        {
            "number": 7,
            "title": "Boundary PR",
            "url": "url7",
            "closedAt": "2025-07-31T00:00:00Z",
            "files": [{"path": "my-charm/src/charm.py"}],
        },
        # 8. On the boundary (start_ts) - should NOT be included because of `<`
        {
            "number": 8,
            "title": "Boundary PR 2",
            "url": "url8",
            "closedAt": "2025-07-01T00:00:00Z",
            "files": [{"path": "my-charm/src/charm.py"}],
        },
    ]
    mock_run_gh_pr_list.return_value = mock_prs_data

    matched_prs = get_prs("main", charm_name, start_ts, end_ts, "/fake/repo")

    mock_run_gh_pr_list.assert_called_once_with("main", "/fake/repo")

    assert len(matched_prs) == 3
    pr_numbers = [pr["number"] for pr in matched_prs]
    assert pr_numbers == [1, 6, 7]


@patch("cephtools.reltool.os.remove")
@patch("cephtools.reltool.zipfile.ZipFile")
@patch("cephtools.reltool.subprocess.check_call")
@patch("cephtools.reltool.tempfile.NamedTemporaryFile")
@patch("cephtools.reltool.os.path.expanduser")
@patch("cephtools.reltool.os.makedirs")
def test_download_and_get_ts(
    mock_makedirs,
    mock_expanduser,
    mock_named_temp_file,
    mock_check_call,
    mock_zipfile,
    mock_os_remove,
):
    """Verify that download_and_get_ts works correctly."""
    charm = "my-charm"
    channel = "stable"
    base = "ubuntu@22.04"
    juju_tmp = "/fake/juju/tmp"
    tmp_charm_path = f"{juju_tmp}/tmpfile.charm"
    commit_date_str = "2025-07-07T12:00:00+00:00"
    expected_ts = datetime.fromisoformat(commit_date_str)

    mock_expanduser.return_value = juju_tmp

    # Mock NamedTemporaryFile to return a specific path
    mock_temp_file_obj = MagicMock()
    mock_temp_file_obj.name = tmp_charm_path
    mock_named_temp_file.return_value = mock_temp_file_obj

    # Mock zipfile to simulate reading git-info.txt
    git_info_content = f"commit_date: {commit_date_str}\n"
    mock_zip_file_context = MagicMock()
    mock_zip_file_context.__enter__.return_value.open.return_value.__enter__.return_value = [
        line.encode() for line in git_info_content.splitlines(True)
    ]
    mock_zipfile.return_value = mock_zip_file_context

    # Call the function
    ts = download_and_get_ts(charm, channel, base)

    # Assertions
    mock_expanduser.assert_called_once_with("~/snap/juju/common")
    mock_makedirs.assert_called_once_with(juju_tmp, exist_ok=True)
    mock_named_temp_file.assert_called_once_with(
        suffix=".charm", delete=False, dir=juju_tmp
    )
    mock_check_call.assert_called_once_with(
        [
            "juju",
            "download",
            charm,
            "--channel",
            channel,
            "--base",
            base,
            "--filepath",
            tmp_charm_path,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    mock_zipfile.assert_called_once_with(tmp_charm_path)
    mock_os_remove.assert_called_once_with(tmp_charm_path)
    assert ts == expected_ts


@patch("cephtools.reltool.subprocess.run")
def test_run_charmcraft_status(mock_run):
    """Verify that run_charmcraft_status calls charmcraft and parses json."""
    charm_name = "my-charm"
    mock_proc = MagicMock()
    mock_proc.stdout = '{"key": "value"}'
    mock_run.return_value = mock_proc

    result = run_charmcraft_status(charm_name)

    mock_run.assert_called_once_with(
        ["charmcraft", "status", charm_name, "--format", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result == {"key": "value"}


@patch("cephtools.reltool.subprocess.run")
def test_run_charmcraft_status_bad_json(mock_run):
    """Verify that run_charmcraft_status raises JSONDecodeError for bad JSON."""
    charm_name = "my-charm"
    mock_proc = MagicMock()
    mock_proc.stdout = "this is not json"
    mock_run.return_value = mock_proc

    with pytest.raises(json.JSONDecodeError):
        run_charmcraft_status(charm_name)


@patch("cephtools.reltool.subprocess.run")
def test_run_charmcraft_status_process_error(mock_run):
    """Verify that run_charmcraft_status propagates CalledProcessError."""
    mock_run.side_effect = subprocess.CalledProcessError(1, "cmd")
    charm_name = "my-charm"
    with pytest.raises(subprocess.CalledProcessError):
        run_charmcraft_status(charm_name)


@patch("cephtools.reltool.subprocess.run")
@patch("cephtools.reltool.run_charmcraft_status")
def test_charm_rel(mock_run_charmcraft_status, mock_subprocess_run):
    """Verify charm_rel finds and releases charms, handling errors."""
    source = "stable"
    target = "candidate"
    base = "ubuntu@22.04"
    # charm1: success
    # charm2: charmcraft status fails
    # charm3: charmcraft release fails
    # charm4: no matching revision
    charms = ("charm1", "charm2", "charm3", "charm4")

    status_charm1 = [
        {
            "mappings": [
                {
                    "base": {"channel": base},
                    "releases": [{"channel": source, "revision": 101}],
                }
            ]
        }
    ]
    status_charm3 = [
        {
            "mappings": [
                {
                    "base": {"channel": base},
                    "releases": [{"channel": source, "revision": 301}],
                }
            ]
        }
    ]
    status_charm4 = [
        {
            "mappings": [
                {
                    "base": {"channel": base},
                    "releases": [{"channel": "other", "revision": 401}],
                }
            ]
        }
    ]

    mock_run_charmcraft_status.side_effect = [
        status_charm1,
        subprocess.CalledProcessError(1, "cmd"),
        status_charm3,
        status_charm4,
    ]

    mock_subprocess_run.side_effect = [
        MagicMock(),  # for charm1 release
        subprocess.CalledProcessError(1, "cmd"),  # for charm3 release
    ]

    runner = CliRunner()
    result = runner.invoke(charm_rel, [source, target, base, *charms, "--apply"])

    assert result.exit_code == 0
    assert result.exception is None

    # check calls to run_charmcraft_status
    assert mock_run_charmcraft_status.call_args_list == [
        call("charm1"),
        call("charm2"),
        call("charm3"),
        call("charm4"),
    ]

    # check calls to charmcraft release
    assert mock_subprocess_run.call_count == 2
    mock_subprocess_run.assert_any_call(
        ["charmcraft", "release", "-r", "101", "-c", target, "charm1"],
        check=True,
    )
    mock_subprocess_run.assert_any_call(
        ["charmcraft", "release", "-r", "301", "-c", target, "charm3"],
        check=True,
    )

    output = result.output

    # charm1 success
    assert "charm1 101" in output

    # charm2 status failure
    assert "Could not get status for charm charm2" in output

    # charm3 release failure
    assert "charm3 301" in output
    assert "Failed to release charm charm3 revision 301" in output

    # charm4 no revision
    assert "charm4" in output
    assert "401" not in output


@patch("cephtools.reltool.subprocess.run")
@patch("cephtools.reltool.run_charmcraft_status")
def test_charm_rel_dry_run(mock_run_charmcraft_status, mock_subprocess_run):
    """Verify charm_rel dry-run mode works correctly."""
    source = "stable"
    target = "candidate"
    base = "ubuntu@22.04"
    charms = ("charm1",)

    status_charm1 = [
        {
            "mappings": [
                {
                    "base": {"channel": base},
                    "releases": [{"channel": source, "revision": 101}],
                }
            ]
        }
    ]
    mock_run_charmcraft_status.return_value = status_charm1

    runner = CliRunner()
    result = runner.invoke(charm_rel, [source, target, base, *charms])

    assert result.exit_code == 0
    assert result.exception is None

    mock_run_charmcraft_status.assert_called_once_with("charm1")
    mock_subprocess_run.assert_not_called()

    output = result.output
    assert "would release charm1 101 to candidate" in output
