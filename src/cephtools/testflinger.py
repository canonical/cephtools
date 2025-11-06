from __future__ import annotations

import dataclasses
import datetime as dt
import json
import subprocess
import uuid
from collections import deque
from pathlib import Path
from typing import Callable, Iterable

import click

from cephtools.config import (
    DEFAULT_TESTFLINGER_CONFIG_PATH,
    DEFAULT_TESTFLINGER_DEPLOY_RESERVE_FOR,
    DEFAULT_TESTFLINGER_RESERVE_FOR,
    load_nested_yaml,
)
from cephtools.state import get_state_file


DEFAULT_CONFIG_PATH = DEFAULT_TESTFLINGER_CONFIG_PATH
DEFAULT_RESERVE_FOR = DEFAULT_TESTFLINGER_RESERVE_FOR
DEFAULT_DEPLOY_RESERVE_FOR = DEFAULT_TESTFLINGER_DEPLOY_RESERVE_FOR

RESERVATION_PREFIXES = [
    "*** TESTFLINGER SYSTEM RESERVED ***",
    "You can now connect to ",
    "Current time:           [",
    "Reservation expires at: [",
    "Reservation will automatically timeout in ",
    "To end the reservation sooner use: testflinger-cli cancel ",
]

Runner = Callable[..., subprocess.CompletedProcess]


@dataclasses.dataclass
class BackendConfig:
    launchpad_account: str
    job_tag: str | None = None
    mattermost_name: str | None = None


@dataclasses.dataclass
class ReservationDetails:
    job_id: str
    queue_name: str
    user: str
    ip: str
    expires_at: dt.datetime
    timeout_seconds: int


def _load_simple_yaml(path: Path) -> dict[str, str | None]:
    data: dict[str, str | None] = {}
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if ":" not in stripped:
            raise click.ClickException(f"Invalid config line: '{line}'")
        key, raw_value = stripped.split(":", 1)
        value = raw_value.strip()
        if value.lower() in {"null", "none", "~", ""}:
            data[key.strip()] = None
            continue
        if (value.startswith("'") and value.endswith("'")) or (
            value.startswith('"') and value.endswith('"')
        ):
            value = value[1:-1]
        data[key.strip()] = value
    return data


def load_backend_config(path: Path) -> BackendConfig:
    data = _load_simple_yaml(path)
    try:
        launchpad_account = data["launchpad_account"]
    except KeyError as exc:  # pragma: no cover - defensive
        raise click.ClickException(
            f"Missing required key {exc!s} in {path}"
        ) from exc
    if launchpad_account is None:
        raise click.ClickException(
            f"Incomplete configuration in {path}: "
            "launchpad_account must be set."
        )
    return BackendConfig(
        launchpad_account=launchpad_account,
        job_tag=data.get("job_tag"),
        mattermost_name=data.get("mattermost_name"),
    )


def read_vmaas_network_config(path: Path | None = None) -> dict[str, object]:
    target = Path(path) if path is not None else get_state_file("network.yaml")
    target = target.expanduser()
    if not target.exists():
        raise click.ClickException(f"Expected configuration file at {target}")
    data = load_nested_yaml(target)
    try:
        network = data["network"]
    except KeyError as exc:
        raise click.ClickException(
            f"{path} is missing the 'network' section."
        ) from exc
    if not isinstance(network, dict):
        raise click.ClickException(
            f"{path} has unexpected structure for the 'network' section."
        )
    return network


def read_vmaas_cloud_config(path: Path | None = None) -> dict[str, object]:
    target = Path(path) if path is not None else get_state_file("cloud.yaml")
    target = target.expanduser()
    if not target.exists():
        raise click.ClickException(f"Expected configuration file at {target}")
    data = load_nested_yaml(target)
    try:
        return data["clouds"]
    except KeyError as exc:
        raise click.ClickException(
            f"{path} is missing the 'clouds' section."
        ) from exc


def read_vmaas_credentials(path: Path | None = None) -> dict[str, object]:
    target = Path(path) if path is not None else get_state_file("cred.yaml")
    target = target.expanduser()
    if not target.exists():
        raise click.ClickException(f"Expected configuration file at {target}")
    data = load_nested_yaml(target)
    try:
        return data["credentials"]
    except KeyError as exc:
        raise click.ClickException(
            f"{path} is missing the 'credentials' section."
        ) from exc


def machine_ids(count: int, offset: int = 0) -> list[str]:
    if count <= 0:
        raise click.ClickException("count must be a positive integer.")
    if offset < 0:
        raise click.ClickException("offset must be zero or a positive integer.")

    clouds = read_vmaas_cloud_config()
    if "maas-cloud" not in clouds or not isinstance(clouds["maas-cloud"], dict):
        raise click.ClickException(
            "cloud.yaml is missing maas-cloud configuration."
        )

    credentials = read_vmaas_credentials()
    try:
        cloud_creds = credentials["maas-cloud"]
    except KeyError as exc:
        raise click.ClickException(
            "cred.yaml is missing maas-cloud credentials."
        ) from exc

    if not isinstance(cloud_creds, dict) or not cloud_creds:
        raise click.ClickException(
            "cred.yaml does not define any maas-cloud credentials."
        )

    profile, profile_details = next(iter(cloud_creds.items()))
    if not isinstance(profile_details, dict):
        raise click.ClickException(
            "cred.yaml has unexpected structure for maas-cloud credentials."
        )

    read_vmaas_network_config()  # ensure file exists/valid; not directly used here.

    cmd = [
        "maas",
        profile,
        "machines",
        "read",
    ]
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip() if exc.stderr else "unknown error"
        raise click.ClickException(
            f"Failed to query MAAS machines: {stderr}"
        ) from exc

    try:
        machines = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise click.ClickException("Failed to parse MAAS machines JSON.") from exc

    if not isinstance(machines, list):
        raise click.ClickException("Unexpected MAAS machines response format.")

    if offset >= len(machines):
        return []
    selected = machines[offset : offset + count]
    return [str(machine.get("system_id")) for machine in selected if machine.get("system_id")]


def save_backend_config(path: Path, config: BackendConfig) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"launchpad_account: {config.launchpad_account}",
        f"job_tag: {config.job_tag if config.job_tag is not None else 'null'}",
        "mattermost_name: "
        + (
            str(config.mattermost_name)
            if config.mattermost_name is not None
            else "null"
        ),
        "",
    ]
    path.write_text("\n".join(lines))


def ensure_backend_config(
    path: Path,
    launchpad_account: str | None,
    job_tag: str | None,
    mattermost_name: str | None,
) -> tuple[BackendConfig, bool]:
    if path.exists():
        if any(
            value is not None
            for value in (
                launchpad_account,
                job_tag,
                mattermost_name,
            )
        ):
            raise click.ClickException(
                f"{path} already exists; remove it or omit config overrides."
            )
        return (load_backend_config(path), False)

    if launchpad_account is None:
        raise click.ClickException(
            "Configuration file is missing. Provide --launchpad-account."
        )
    config = BackendConfig(
        launchpad_account=launchpad_account,
        job_tag=job_tag,
        mattermost_name=mattermost_name,
    )
    save_backend_config(path, config)
    click.echo(f"Saved configuration to {path}")
    click.echo(
        "Run the command again to reserve a queue, now that the config exists."
    )
    return (config, True)


def build_job_file(config: BackendConfig, queue_name: str, reserve_for: int) -> str:
    lines: list[str] = []
    if config.mattermost_name:
        lines.append(
            f"# Ask {config.mattermost_name} on Mattermost if you have questions"
        )
    if config.job_tag:
        lines.append("tags:")
        lines.append(f"  - {config.job_tag}")
        lines.append("")
    lines.append(f"job_queue: {queue_name}")
    lines.append("")
    lines.append("provision_data:")
    lines.append("  distro: noble")
    lines.append("")
    lines.append("reserve_data:")
    lines.append("  ssh_keys:")
    lines.append(f"    - {config.launchpad_account}")
    lines.append(f"  timeout: {reserve_for}")
    lines.append("")
    return "\n".join(lines)


def write_job_file(config: BackendConfig, queue_name: str, reserve_for: int) -> Path:
    job_contents = build_job_file(config, queue_name, reserve_for)
    base_dir = Path.home()
    job_path = base_dir / f"reserve-{queue_name}-{uuid.uuid4().hex}.yaml"
    job_path.write_text(job_contents)
    return job_path


def parse_submit_output(stdout: str) -> str:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if len(lines) < 2 or lines[0] != "Job submitted successfully!":
        raise click.ClickException(
            "Unexpected output from testflinger submit:\n" + stdout
        )
    parts = lines[1].split()
    if len(parts) < 2:
        raise click.ClickException(
            "Could not extract job id from testflinger output."
        )
    return parts[-1]


def submit_reserve_job(
    config: BackendConfig,
    queue_name: str,
    reserve_for: int,
    runner: Runner,
    testflinger_bin: str,
) -> str:
    job_file = write_job_file(config, queue_name, reserve_for)
    try:
        result = runner(
            [testflinger_bin, "submit", str(job_file)],
            capture_output=True,
            text=True,
            check=False,
        )
    finally:
        job_file.unlink(missing_ok=True)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        message = stderr or stdout or "testflinger submit failed"
        raise click.ClickException(message)
    return parse_submit_output(result.stdout or "")


def _parse_reservation_window(
    window: Iterable[str],
    queue_name: str,
) -> ReservationDetails | None:
    window_list = list(window)
    if len(window_list) != len(RESERVATION_PREFIXES):
        return None
    stripped: list[str] = []
    for line, prefix in zip(window_list, RESERVATION_PREFIXES):
        if not line.startswith(prefix):
            return None
        stripped.append(line[len(prefix) :].strip())
    user_at_ip = stripped[1]
    if "@" not in user_at_ip:
        return None
    user, ip = user_at_ip.split("@", 1)
    current_time = stripped[2].rstrip("]")
    expires_at = stripped[3].rstrip("]")
    timeout_part = stripped[4].split()
    if not timeout_part:
        return None
    job_id = stripped[5].split()[-1]
    try:
        expires_dt = dt.datetime.fromisoformat(expires_at)
        _ = dt.datetime.fromisoformat(current_time)
        timeout_seconds = int(timeout_part[0])
    except (ValueError, IndexError):
        return None
    return ReservationDetails(
        job_id=job_id,
        queue_name=queue_name,
        user=user,
        ip=ip,
        expires_at=expires_dt,
        timeout_seconds=timeout_seconds,
    )


def await_reservation_details(
    queue_name: str,
    job_id: str,
    testflinger_bin: str,
    echo: Callable[[str], None],
) -> ReservationDetails:
    proc = subprocess.Popen(
        [testflinger_bin, "poll", job_id],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    if proc.stdout is None or proc.stderr is None:  # pragma: no cover
        proc.kill()
        raise click.ClickException("Failed to capture testflinger output.")

    window: deque[str] = deque(maxlen=len(RESERVATION_PREFIXES))
    details: ReservationDetails | None = None

    try:
        for line in proc.stdout:
            stripped = line.rstrip("\n")
            echo(stripped)
            window.append(stripped)
            maybe_details = _parse_reservation_window(window, queue_name)
            if maybe_details is not None:
                details = maybe_details
                break
    finally:
        if proc.poll() is None:
            proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive
            proc.kill()
            proc.wait()

    stderr_output = proc.stderr.read().strip()
    proc.stdout.close()
    proc.stderr.close()

    if details is None:
        message = "Failed to identify reservation details in testflinger output."
        if stderr_output:
            message += f" Stderr: {stderr_output}"
        raise click.ClickException(message)

    if details.job_id != job_id:
        raise click.ClickException(
            "Mismatch between job id reported by submit and poll output."
        )

    return details


def reserve_node(
    queue_name: str,
    reserve_for: int,
    config: BackendConfig,
    testflinger_bin: str,
    runner: Runner,
    echo: Callable[[str], None],
) -> ReservationDetails:
    job_id = submit_reserve_job(
        config,
        queue_name,
        reserve_for,
        runner=runner,
        testflinger_bin=testflinger_bin,
    )
    echo(f"Submitted job {job_id} to reserve {queue_name}. Waiting for details.")
    details = await_reservation_details(
        queue_name=queue_name,
        job_id=job_id,
        testflinger_bin=testflinger_bin,
        echo=echo,
    )
    return details


def _build_ssh_command(details: ReservationDetails) -> str:
    return (
        "ssh -o 'StrictHostKeyChecking=no' "
        "-o 'UserKnownHostsFile=/dev/null' "
        f"'{details.user}@{details.ip}'"
    )


def print_reservation_summary(
    details: ReservationDetails,
    testflinger_bin: str,
    echo: Callable[[str], None],
) -> None:
    echo("")
    echo(
        f"Reserved queue {details.queue_name} under job {details.job_id}. "
        f"Reservation expires at {details.expires_at.isoformat()}."
    )
    echo(f"Connect with: {_build_ssh_command(details)}")
    echo(f"Cancel early with: {testflinger_bin} cancel {details.job_id}")


def build_deploy_script() -> str:
    return "\n".join(
        [
            "set -euxo pipefail",
            "sudo snap install astral-uv --classic",
            "mkdir -p ~/src",
            "cd ~/src",
            "git clone https://github.com/canonical/cephtools.git",
            "cd cephtools/",
            "uv pip install --system --prefix ~/.local .",
            'export PATH="$PATH:$HOME/.local/bin"',
            "cephtools vmaas install",
            "",
        ]
    )


def perform_remote_deploy(
    details: ReservationDetails,
    script: str,
    runner: Runner,
) -> None:
    result = runner(
        [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            f"{details.user}@{details.ip}",
            "bash",
            "-se",
        ],
        input=script,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise click.ClickException(
            f"Remote deployment failed with exit code {result.returncode}."
        )


@click.group()
def cli() -> None:  # pragma: no cover - exercised via click integration tests
    """Testflinger related helpers."""


@cli.command("reserve")
@click.argument("queue_name", required=False, default="ceph-qa-1")
@click.option(
    "--reserve-for",
    type=int,
    default=DEFAULT_RESERVE_FOR,
    show_default=True,
    help="Reservation duration in seconds.",
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path),
    default=DEFAULT_CONFIG_PATH,
    show_default=True,
    help="Path to the backend configuration file.",
)
@click.option("--launchpad-account", help="Launchpad account used for ssh access.")
@click.option("--job-tag", help="Optional job tag to include in submit jobs.")
@click.option(
    "--mattermost-name",
    help="Optional Mattermost handle to add to job comments.",
)
@click.option(
    "--testflinger-bin",
    default="testflinger",
    show_default=True,
    help="Path to the testflinger CLI binary.",
)
def reserve(  # pragma: no cover - exercised via click integration tests
    queue_name: str,
    reserve_for: int,
    config_path: Path,
    launchpad_account: str | None,
    job_tag: str | None,
    mattermost_name: str | None,
    testflinger_bin: str,
) -> None:
    """Reserve a Testflinger queue and configure SSH access."""
    if reserve_for <= 0:
        raise click.ClickException("--reserve-for must be a positive integer.")

    config, created = ensure_backend_config(
        config_path,
        launchpad_account,
        job_tag,
        mattermost_name,
    )
    if created:
        return

    details = reserve_node(
        queue_name=queue_name,
        reserve_for=reserve_for,
        config=config,
        testflinger_bin=testflinger_bin,
        runner=subprocess.run,
        echo=click.echo,
    )

    print_reservation_summary(details, testflinger_bin, click.echo)


@cli.command("deploy")
@click.argument("queue_name", required=False, default="ceph-qa-1")
@click.option(
    "--reserve-for",
    type=int,
    default=DEFAULT_RESERVE_FOR,
    show_default=True,
    help="Reservation duration in seconds.",
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path),
    default=DEFAULT_CONFIG_PATH,
    show_default=True,
    help="Path to the backend configuration file.",
)
@click.option("--launchpad-account", help="Launchpad account used for ssh access.")
@click.option("--job-tag", help="Optional job tag to include in submit jobs.")
@click.option(
    "--mattermost-name",
    help="Optional Mattermost handle to add to job comments.",
)
@click.option(
    "--testflinger-bin",
    default="testflinger",
    show_default=True,
    help="Path to the testflinger CLI binary.",
)
def deploy(  # pragma: no cover - exercised via click integration tests
    queue_name: str,
    reserve_for: int,
    config_path: Path,
    launchpad_account: str | None,
    job_tag: str | None,
    mattermost_name: str | None,
    testflinger_bin: str,
) -> None:
    """Reserve a queue and install cephtools + VMaaS on it."""
    if reserve_for <= 0:
        raise click.ClickException("--reserve-for must be a positive integer.")

    config, created = ensure_backend_config(
        config_path,
        launchpad_account,
        job_tag,
        mattermost_name,
    )
    if created:
        return

    details = reserve_node(
        queue_name=queue_name,
        reserve_for=reserve_for,
        config=config,
        testflinger_bin=testflinger_bin,
        runner=subprocess.run,
        echo=click.echo,
    )

    print_reservation_summary(details, testflinger_bin, click.echo)

    click.echo("")
    click.echo("Configuring remote environment for VMaaS deployment.")
    script = build_deploy_script()
    try:
        perform_remote_deploy(
            details=details,
            script=script,
            runner=subprocess.run,
        )
    except click.ClickException as exc:
        raise click.ClickException(
            "Failed to deploy VMaaS on queue "
            f"{details.queue_name} ({details.ip}): {exc.message}"
        ) from exc

    click.echo("Remote deployment succeeded. VMaaS should now be installed.")
    click.echo(f"Connect with: {_build_ssh_command(details)}")
