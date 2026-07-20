#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import re
import secrets
import shlex
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from ipaddress import ip_interface, ip_network
from pathlib import Path

import click
import jubilant
from cephtools.common import ensure_snap, run
from cephtools.progress import (
    emit,
    install_fault_handlers,
    mark_complete,
    operation,
)
from cephtools.state import get_state_file
from cephtools.terraform import ensure_terragrunt, terraform_root_candidates
from cephtools.testflinger import (
    read_testenv_cloud_config,
    read_testenv_credentials,
    read_testenv_network_config,
)

# Fixed conventions for disposable test environments.
DEFAULT_MAAS_VERSION = "3.7"
DEFAULT_SUBSTRATE = "lxd"
DEFAULT_MAAS_VM_CPUS = 8
DEFAULT_MAAS_VM_MEMORY = "16GiB"
DEFAULT_MAAS_VM_DISK = "80GiB"
DEFAULT_MAAS_VM_IMAGE = "ubuntu:24.04"
MAAS_ADMIN = "admin"
MAAS_ADMIN_PASSWORD = "maaspass"
MAAS_ADMIN_EMAIL = "admin@example.com"
LXD_BRIDGE = "lxdbr0"
MAAS_LXD_BRIDGE = "maasbr0"
MAAS_VM_NAME = "maas-vm"
MAAS_LXD_PROJECT = "maas"
MAAS_VM_HOST = "local-lxd"
CEPHTOOLS_TAG = "cephtools"
CEPHTOOLS_MODEL = "cephtools"
MAAS_CONTROLLER = "maas-controller"
LXD_CONTROLLER = "lxd-controller"
REQUIRED_BOOT_ARCHITECTURE = "amd64/generic"
EXT_LXD_NETWORK = "ext"
EXTERNAL_SPACE_NAME = "external"
JUJU_SPACE_NAME = "jujuspace"
ENSURE_NODES_INPUT_FILENAME = "ensure-nodes.hcl"
MAAS_DB_NAME = "maasdb"
MAAS_DB_USER = "maas"
MAAS_DB_HOST = "localhost"
MAAS_DB_PORT = "5432"
DNS_PRECHECK_HOSTS = (
    "archive.ubuntu.com",
    "security.ubuntu.com",
    "registry.terraform.io",
)
DNS_PRECHECK_TIMEOUT_SECONDS = 120
DNS_PRECHECK_INTERVAL_SECONDS = 5
BIND9_STOP_TIMEOUT_SECONDS = 30
BIND9_STOP_INTERVAL_SECONDS = 1
LXD_INIT_RETRY_DELAY_SECONDS = 2
WARMUP_VM_NAME = "warmup-vm"
SUBSTRATE_MAAS_HOST = "maas-host"
SUBSTRATE_MAAS_VM = "maas-vm"
SUBSTRATE_LXD = "lxd"
SUBSTRATES = (SUBSTRATE_MAAS_HOST, SUBSTRATE_MAAS_VM, SUBSTRATE_LXD)
MAAS_VM_BOOTSTRAP_SCRIPT = "/tmp/cephtools-maas-vm-bootstrap.sh"
TESTENV_STATE_FILENAMES = ("cloud.yaml", "cred.yaml", "network.yaml")
USER_JUJU_STATE_PATHS = (
    Path("~/.local/share/juju").expanduser(),
    Path("~/.cache/juju").expanduser(),
    Path("~/.config/juju").expanduser(),
    Path("~/.local/state/juju").expanduser(),
)
TESTENV_ROOT_RESIDUAL_PATHS = (
    "/var/snap/lxd",
    "/var/lib/lxd",
    "/etc/lxd",
    "/var/snap/maas",
    "/etc/maas",
    "/var/lib/maas",
    "/var/log/maas",
    "/etc/bind/maas",
    "/var/lib/bind/maas",
    "/etc/postgresql",
    "/var/lib/postgresql",
    "/var/log/postgresql",
)


@dataclass(frozen=True)
class CleanupPhaseResult:
    phase: str
    outcome: str
    detail: str

    @property
    def failed(self) -> bool:
        return self.outcome == "failed"


def _format_juju_error(exc: jubilant.CLIError) -> str:
    stderr = (getattr(exc, "stderr", "") or "").strip()
    stdout = (getattr(exc, "output", "") or "").strip()
    if stderr:
        return stderr
    if stdout:
        return stdout
    return f"exit code {getattr(exc, 'returncode', 'unknown')}"


def _format_process_error(
    proc: subprocess.CalledProcessError | subprocess.CompletedProcess[str],
) -> str:
    stderr = (getattr(proc, "stderr", "") or "").strip()
    stdout = (getattr(proc, "stdout", "") or "").strip()
    if stderr:
        return stderr
    if stdout:
        return stdout
    return f"exit code {getattr(proc, 'returncode', 'unknown')}"


def _message_indicates_not_found(message: str) -> bool:
    normalized = message.lower()
    return any(
        marker in normalized
        for marker in (
            "not found",
            "does not exist",
            "doesn't exist",
            "no such",
            "missing",
            "no matching snaps installed",
        )
    )


def _is_maas_substrate(substrate: str) -> bool:
    return substrate in {SUBSTRATE_MAAS_HOST, SUBSTRATE_MAAS_VM}


def _controller_name(substrate: str) -> str:
    return LXD_CONTROLLER if substrate == SUBSTRATE_LXD else MAAS_CONTROLLER


def _cloud_name(substrate: str) -> str:
    return "localhost" if substrate == SUBSTRATE_LXD else "maas-cloud"


def _model_constraint(substrate: str) -> str | None:
    if substrate == SUBSTRATE_LXD:
        return "virt-type=virtual-machine"
    return f"tags={CEPHTOOLS_TAG}"


def _resolve_terragrunt_dir() -> Path:
    env_path = os.getenv("CEPHTOOLS_TERRAGRUNT_DIR")
    candidates: list[Path] = []
    if env_path:
        candidates.append(Path(env_path).expanduser())

    for root_candidate in terraform_root_candidates():
        candidates.append(Path(root_candidate).expanduser() / "maas-nodes")

    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.is_dir():
            return resolved

    attempted = "\n  - ".join(str(c.resolve()) for c in seen)
    raise click.ClickException(
        "Unable to locate terragrunt configuration directory.\n"
        "Checked the following locations:\n"
        f"  - {attempted}\n"
        "Set CEPHTOOLS_TERRAGRUNT_DIR to override."
    )


def _format_hcl_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value)


def _write_ensure_nodes_inputs_file(
    terragrunt_dir: Path,
    inputs: dict[str, object],
) -> Path:
    inputs_path = terragrunt_dir / ENSURE_NODES_INPUT_FILENAME
    lines = ["inputs = {"]
    for key, value in inputs.items():
        lines.append(f"  {key} = {_format_hcl_value(value)}")
    lines.append("}")
    contents = "\n".join(lines) + "\n"

    tmp_path = inputs_path.with_name(f".{inputs_path.name}.tmp")
    tmp_path.write_text(contents)
    os.chmod(tmp_path, 0o600)
    os.replace(tmp_path, inputs_path)
    return inputs_path


def _terragrunt_vm_hostnames(terragrunt_dir: Path) -> list[str]:
    result = run(
        f"cd {shlex.quote(str(terragrunt_dir))} && terragrunt output -json",
        shell=True,
    )
    try:
        outputs = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise click.ClickException(
            "Failed to parse terragrunt outputs as JSON."
        ) from exc

    hostnames_value = outputs.get("vm_hostnames")
    if not isinstance(hostnames_value, dict) or "value" not in hostnames_value:
        raise click.ClickException("Terragrunt outputs did not include vm_hostnames.")

    hostnames = hostnames_value["value"]
    if not isinstance(hostnames, list):
        raise click.ClickException("Terragrunt vm_hostnames output must be a list.")

    return [str(hostname) for hostname in hostnames]


def _ensure_maas_tag(admin: str, tag: str, *, maas_vm_name: str | None = None) -> None:
    result = _run_maas_cli(
        f"maas {shlex.quote(admin)} tags read",
        maas_vm_name=maas_vm_name,
    )
    try:
        tags = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise click.ClickException("Failed to parse MAAS tags output as JSON.") from exc

    for entry in tags:
        if isinstance(entry, dict) and entry.get("name") == tag:
            return

    _run_maas_cli(
        f"maas {shlex.quote(admin)} tags create name={tag}",
        maas_vm_name=maas_vm_name,
    )


def _tag_maas_machines(
    admin: str,
    hostnames: list[str],
    tag: str,
    *,
    maas_vm_name: str | None = None,
) -> dict[str, str]:
    if not hostnames:
        return {}

    result = _run_maas_cli(
        f"maas {shlex.quote(admin)} machines read",
        maas_vm_name=maas_vm_name,
    )
    try:
        machines = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise click.ClickException(
            "Failed to parse MAAS machines output as JSON."
        ) from exc

    hostname_to_system_id = {
        str(machine.get("hostname")): machine.get("system_id")
        for machine in machines
        if isinstance(machine, dict)
        and machine.get("hostname")
        and machine.get("system_id")
    }

    missing: list[str] = []
    for hostname in hostnames:
        system_id = hostname_to_system_id.get(hostname)
        if not system_id:
            missing.append(hostname)
            continue
        _run_maas_cli(
            f"maas {shlex.quote(admin)} tag update-nodes {tag} add={system_id}",
            maas_vm_name=maas_vm_name,
        )

    if missing:
        click.echo(
            "Warning: Unable to tag machines not found in MAAS: "
            + ", ".join(sorted(missing)),
            err=True,
        )

    return hostname_to_system_id


def _tag_data_disks(
    admin: str,
    hostnames: list[str],
    hostname_to_system_id: dict[str, str],
    *,
    tag: str,
    maas_vm_name: str | None = None,
) -> None:
    for hostname in hostnames:
        system_id = hostname_to_system_id.get(hostname)
        if not system_id:
            continue

        result = _run_maas_cli(
            f"maas {shlex.quote(admin)} block-devices read {system_id}",
            maas_vm_name=maas_vm_name,
        )
        try:
            devices = json.loads(result.stdout or "[]")
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive
            raise click.ClickException(
                f"Failed to parse block devices for machine {system_id}."
            ) from exc

        for device in devices:
            if not isinstance(device, dict):
                continue
            if device.get("used_for") != "Unused":
                continue
            device_id = device.get("id")
            if device_id is None:
                continue

            _run_maas_cli(
                f"maas {shlex.quote(admin)} block-device add-tag "
                f"{system_id} {device_id} tag={tag}",
                maas_vm_name=maas_vm_name,
            )


def _ensure_juju_model(
    model: str, *, controller: str = MAAS_CONTROLLER, constraint: str | None = None
) -> None:
    juju = jubilant.Juju()
    try:
        models_output = juju.cli(
            "models",
            "--format",
            "json",
            "--controller",
            controller,
            include_model=False,
        )
    except jubilant.CLIError as exc:
        message = _format_juju_error(exc)
        raise click.ClickException(f"Failed to list Juju models: {message}") from exc

    try:
        payload = json.loads(models_output or "{}")
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise click.ClickException(
            "Failed to parse Juju models output as JSON."
        ) from exc

    models = payload.get("models")
    if not isinstance(models, list):
        models = []

    existing = any(
        isinstance(entry, dict) and entry.get("name") == model for entry in models
    )
    if not existing:
        try:
            juju.add_model(model, controller=controller)
        except jubilant.CLIError as exc:
            message = _format_juju_error(exc)
            raise click.ClickException(
                f"Failed to add Juju model '{model}': {message}"
            ) from exc

    if constraint is None:
        return

    juju_for_model = jubilant.Juju(model=f"{controller}:{model}")
    try:
        juju_for_model.cli("set-model-constraints", constraint)
    except jubilant.CLIError as exc:
        message = _format_juju_error(exc)
        raise click.ClickException(
            f"Failed to set constraints for model '{model}': {message}"
        ) from exc


def primary_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        out = run("hostname -I", check=True)
        return out.stdout.strip().split()[0]


def lxd_ready():
    try:
        run("sudo lxd waitready", check=True)
    except subprocess.CalledProcessError as e:
        print(e.stderr)


def install_maas_deb(version: str) -> None:
    run(
        [
            "sudo",
            "apt-get",
            "-y",
            "install",
            "software-properties-common",
            "postgresql",
        ]
    )
    run(
        ["sudo", "apt-get", "-y", "remove", "systemd-timesyncd"],
        check=False,
    )
    run(["sudo", "apt-add-repository", "-y", f"ppa:maas/{version}"])
    run(["sudo", "apt-get", "update"])
    run(["sudo", "apt-get", "-y", "install", "maas"])


def _bind9_excluded_interface_names() -> set[str]:
    # MAAS-managed guests on both LXD bridges need MAAS internal DNS (for
    # example *.maas-internal) during deployment, so bind9 must listen on both
    # bridge addresses.
    return set()


def _bind9_ipv4_listen_addresses() -> list[str]:
    result = run(["ip", "-j", "-4", "addr", "show"])
    try:
        interfaces = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise click.ClickException(
            "Failed to parse IPv4 interface addresses as JSON."
        ) from exc

    excluded_interfaces = _bind9_excluded_interface_names()
    addresses: list[str] = ["127.0.0.1"]
    seen = {"127.0.0.1"}
    for interface in interfaces:
        if not isinstance(interface, dict):
            continue
        ifname = interface.get("ifname")
        if isinstance(ifname, str) and ifname in excluded_interfaces:
            continue
        addr_info = interface.get("addr_info") or []
        if not isinstance(addr_info, list):
            continue
        for address in addr_info:
            if not isinstance(address, dict):
                continue
            if address.get("family") != "inet":
                continue
            local = address.get("local")
            if not local:
                continue
            local = str(local)
            if local in seen:
                continue
            seen.add(local)
            addresses.append(local)

    return addresses


def configure_maas_bind9_ipv4() -> None:
    listen_addresses = _bind9_ipv4_listen_addresses()
    rendered_addresses = " ".join(f"{address};" for address in listen_addresses)
    desired_listen_on = f"    listen-on {{ {rendered_addresses} }};"
    click.echo(
        "Configuring MAAS bind9 IPv4 listen-on policy on detected addresses: "
        + ", ".join(listen_addresses)
    )
    run(
        "sudo python3 - <<'PY'\n"
        "from datetime import datetime, timezone\n"
        "from pathlib import Path\n"
        "import re\n"
        "import shutil\n"
        "\n"
        "path = Path('/etc/bind/named.conf.options')\n"
        f"desired = {desired_listen_on!r}\n"
        "marker = 'include \"/etc/bind/maas/named.conf.options.inside.maas\";'\n"
        "text = path.read_text(encoding='ascii')\n"
        "pattern = re.compile(r'^[\\t ]*listen-on\\s+\\{[^}]*\\};[\\t ]*$', re.MULTILINE)\n"
        "if desired in text:\n"
        "    raise SystemExit(0)\n"
        "if pattern.search(text):\n"
        "    new_text = pattern.sub(desired, text, count=1)\n"
        "elif marker in text:\n"
        "    new_text = text.replace(marker, desired + '\\n    ' + marker, 1)\n"
        "else:\n"
        "    idx = text.rfind('};')\n"
        "    if idx == -1:\n"
        "        raise SystemExit('Unable to locate options block terminator in named.conf.options')\n"
        "    new_text = text[:idx] + desired + '\\n' + text[idx:]\n"
        "if new_text == text:\n"
        "    raise SystemExit(0)\n"
        "backup = path.with_name(path.name + '.' + datetime.now(timezone.utc).isoformat())\n"
        "shutil.copy2(path, backup)\n"
        "path.write_text(new_text, encoding='ascii')\n"
        "PY",
        shell=True,
    )
    run(["sudo", "named-checkconf"])
    run(["sudo", "systemctl", "reload", "bind9"])


def _set_lxd_network_no_dns_or_dhcp(name: str) -> None:
    for key, value in (
        ("dns.mode", "none"),
        ("raw.dnsmasq", "port=0"),
        ("ipv4.dhcp", "false"),
        ("ipv6.dhcp", "false"),
    ):
        run(["lxc", "network", "set", name, f"{key}={value}"])


def ensure_lxd_network(name: str, *, ipv4_address: str | None = None) -> None:
    """Compatibility wrapper for the MAAS-owned LXD network helper."""
    ensure_lxd_maas_network(name, ipv4_address=ipv4_address)


def ensure_lxd_maas_network(name: str, *, ipv4_address: str | None = None) -> None:
    nets = json.loads(run("lxc query /1.0/networks").stdout)
    if f"/1.0/networks/{name}" not in nets:
        address_arg = ipv4_address if ipv4_address else "auto"
        run(
            "lxc network create "
            f"{name} "
            f"ipv4.address={address_arg} "
            "ipv4.nat=true "
            "ipv4.dhcp=false "
            "ipv6.address=none "
            "ipv6.dhcp=false "
            "dns.mode=none "
            "raw.dnsmasq=port=0"
        )

    _set_lxd_network_no_dns_or_dhcp(name)


def ensure_lxd_host_network(name: str, *, ipv4_address: str | None = None) -> None:
    """Ensure the normal host LXD bridge keeps LXD DNS/DHCP enabled."""
    nets = json.loads(run("lxc query /1.0/networks").stdout)
    if f"/1.0/networks/{name}" not in nets:
        address_arg = ipv4_address if ipv4_address else "auto"
        run(
            "lxc network create "
            f"{name} "
            f"ipv4.address={address_arg} "
            "ipv4.nat=true "
            "ipv4.dhcp=true "
            "ipv6.address=none "
            "ipv6.dhcp=false "
            "dns.mode=managed"
        )

    for key, value in (
        ("dns.mode", "managed"),
        ("ipv4.dhcp", "true"),
        ("ipv6.dhcp", "false"),
    ):
        run(["lxc", "network", "set", name, f"{key}={value}"])
    # Host-mode setup used raw.dnsmasq=port=0 to disable dnsmasq on MAAS-owned
    # bridges.  When a bridge is restored to host ownership, clear that override
    # so LXD's managed DNS can listen again.
    run(["lxc", "network", "unset", name, "raw.dnsmasq"])


def ensure_lxd_default_profile_network(name: str) -> None:
    profile = json.loads(run("lxc query /1.0/profiles/default").stdout)
    devices = profile.get("devices")
    if not isinstance(devices, dict):
        raise click.ClickException("LXD default profile has unexpected devices data.")

    eth0 = devices.get("eth0")
    if isinstance(eth0, dict) and eth0.get("type") == "nic":
        if eth0.get("network") == name and eth0.get("name") == "eth0":
            return
        run(
            [
                "lxc",
                "profile",
                "device",
                "set",
                "default",
                "eth0",
                f"network={name}",
                "name=eth0",
            ]
        )
        return

    if "eth0" in devices:
        run(["lxc", "profile", "device", "remove", "default", "eth0"])

    run(
        [
            "lxc",
            "profile",
            "device",
            "add",
            "default",
            "eth0",
            "nic",
            f"network={name}",
            "name=eth0",
        ]
    )


def _lxd_project_exists(project: str) -> bool:
    result = run(["lxc", "project", "show", project], check=False, quiet=True)
    return result.returncode == 0


def _default_lxd_storage_pool() -> str:
    result = run(["lxc", "storage", "list", "--format", "json"])
    try:
        pools = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise click.ClickException("Failed to parse LXD storage list JSON.") from exc
    if not pools:
        raise click.ClickException("No LXD storage pool found after lxd init.")
    for pool in pools:
        if pool.get("name") == "default":
            return "default"
    return str(pools[0]["name"])


def _profile_devices(project: str) -> dict[str, object]:
    # Use the REST API instead of parsing ``lxc profile show`` YAML.
    result = run(["lxc", "query", f"/1.0/profiles/default?project={project}"])
    profile = json.loads(result.stdout or "{}")
    devices = profile.get("devices")
    if not isinstance(devices, dict):
        raise click.ClickException(
            f"LXD project {project!r} default profile has unexpected devices data."
        )
    return devices


def _ensure_profile_device(
    project: str, device: str, device_type: str, settings: dict[str, str]
) -> None:
    devices = _profile_devices(project)
    existing = devices.get(device)
    if isinstance(existing, dict) and existing.get("type") == device_type:
        args = [f"{key}={value}" for key, value in settings.items()]
        run(
            [
                "lxc",
                "profile",
                "device",
                "set",
                "default",
                device,
                *args,
                "--project",
                project,
            ]
        )
        return
    if device in devices:
        run(
            [
                "lxc",
                "profile",
                "device",
                "remove",
                "default",
                device,
                "--project",
                project,
            ]
        )
    args = [f"{key}={value}" for key, value in settings.items()]
    run(
        [
            "lxc",
            "profile",
            "device",
            "add",
            "default",
            device,
            device_type,
            *args,
            "--project",
            project,
        ]
    )


def ensure_lxd_maas_project(project: str, network_name: str) -> None:
    """Ensure MAAS-composed LXD VMs use a dedicated project on maasbr0."""
    if not _lxd_project_exists(project):
        run(
            [
                "lxc",
                "project",
                "create",
                project,
                "--debug",
                "-c",
                "features.images=false",
                "-c",
                "features.networks=false",
                "-c",
                "features.profiles=true",
                "-c",
                "features.storage.volumes=true",
            ],
            capture=False,
        )
    storage_pool = _default_lxd_storage_pool()
    _ensure_profile_device(
        project,
        "root",
        "disk",
        {"path": "/", "pool": storage_pool},
    )
    _ensure_profile_device(
        project,
        "eth0",
        "nic",
        {"network": network_name, "name": "eth0"},
    )


def _stop_bind9_for_lxd_setup() -> None:
    click.echo("Stopping bind9 temporarily so LXD bridge setup can claim port 53...")
    run(["sudo", "systemctl", "stop", "bind9"], check=False)


def _bind9_service_state() -> str:
    result = run(["systemctl", "is-active", "bind9"], check=False, quiet=True)
    return result.stdout.strip()


def _bind9_named_processes() -> list[str]:
    result = run("pgrep -a -x named || true", check=False, shell=True, quiet=True)
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _wait_for_bind9_shutdown(
    timeout: float = BIND9_STOP_TIMEOUT_SECONDS,
    interval: float = BIND9_STOP_INTERVAL_SECONDS,
) -> None:
    deadline = time.monotonic() + timeout
    active_states = {"active", "activating", "reloading", "deactivating"}
    attempt = 0

    while True:
        attempt += 1
        state = _bind9_service_state()
        procs = _bind9_named_processes()
        if state not in active_states and not procs:
            return
        remaining = max(0, int(deadline - time.monotonic()))
        emit(
            f"wait_for_bind9_shutdown: attempt {attempt}, state={state}, "
            f"named_procs={len(procs)}, {remaining}s remaining"
        )
        if time.monotonic() >= deadline:
            click.echo(
                "Timed out waiting for bind9 to stop cleanly; continuing and collecting diagnostics if LXD init fails."
            )
            return
        time.sleep(interval)


def _start_bind9_after_lxd_setup() -> None:
    click.echo("Starting bind9 again after LXD bridge setup...")
    run(["sudo", "systemctl", "start", "bind9"], check=False)


def _log_lxd_port_53_diagnostics() -> None:
    click.echo("Collecting LXD/bind9 listener diagnostics...")
    diagnostics = (
        ("bind9 service status", "sudo systemctl status bind9 --no-pager 2>&1 || true"),
        ("named processes", "pgrep -a -x named 2>&1 || true"),
        (
            "port 53 listeners",
            "sudo ss -H -lntup 2>&1 | grep -E '(^tcp|^udp).*:53($|[[:space:]])' || true",
        ),
        (
            "dnsmasq and lxd processes",
            "ps -ef 2>&1 | grep -E 'dnsmasq|lxd' | grep -v grep || true",
        ),
        ("LXD networks", "lxc network list 2>&1 || true"),
        ("ip addresses", "ip -br addr 2>&1 || true"),
    )
    for label, command in diagnostics:
        click.echo(f"-- {label} --")
        result = run(command, check=False, shell=True, quiet=True)
        if output := result.stdout.strip():
            click.echo(output)


def _lxd_is_minimally_initialized() -> bool:
    default_profile = run(
        ["lxc", "query", "/1.0/profiles/default"], check=False, quiet=True
    )
    storage_pools = run(["lxc", "query", "/1.0/storage-pools"], check=False, quiet=True)
    if default_profile.returncode != 0 or storage_pools.returncode != 0:
        return False

    try:
        return bool(json.loads(storage_pools.stdout))
    except ValueError:
        return False


def _run_lxd_minimal_init() -> None:
    attempts = 2
    for attempt in range(1, attempts + 1):
        try:
            run(["sudo", "lxd", "init", "--minimal"])
            return
        except subprocess.CalledProcessError:
            _log_lxd_port_53_diagnostics()
            if _lxd_is_minimally_initialized():
                click.echo(
                    "LXD appears partially initialized despite the init error; continuing with explicit network configuration."
                )
                return
            if attempt < attempts:
                click.echo(
                    "LXD minimal init failed; waiting briefly and retrying once in case a stale port 53 listener is still exiting."
                )
                _wait_for_bind9_shutdown()
                time.sleep(LXD_INIT_RETRY_DELAY_SECONDS)
                continue
            raise


def _wait_for_lxd_daemon_responsive(
    *, timeout: float = 180, interval: float = 3
) -> None:
    """Ensure the LXD daemon is responsive before issuing further ``lxc`` calls.

    ``snap set lxd daemon.user.group=adm`` restarts the LXD daemon
    asynchronously with unpredictable timing: the ``snap set`` returns
    immediately and the restart can land tens of seconds later, causing a
    later ``lxc`` command to block indefinitely on a mid-restart socket
    (observed as reproducible hangs at varying ``lxc`` calls during install).

    Force a synchronous restart of the daemon service and then poll
    ``lxc query /1.0`` until the API responds, with a bounded timeout so a
    genuinely-dead daemon surfaces a failure quickly instead of stalling
    until the job's output timeout.
    """
    # check=False: the service may already be restarting from the snap set.
    run(
        ["sudo", "systemctl", "restart", "snap.lxd.daemon.service"],
        check=False,
    )
    lxd_ready()
    deadline = time.monotonic() + timeout
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        try:
            run(["lxc", "query", "/1.0"], check=True, quiet=True)
            return
        except subprocess.CalledProcessError as exc:
            remaining = max(0, int(deadline - time.monotonic()))
            emit(
                f"wait_for_lxd_daemon: attempt {attempt} not responsive "
                f"(exit {exc.returncode}), {remaining}s remaining"
            )
            time.sleep(interval)
    emit("wait_for_lxd_daemon: timed out, final probe (will raise on failure)")
    # Final probe raises on failure so the failure is visible instead of
    # silently proceeding into commands that will hang.
    run(["lxc", "query", "/1.0"], check=True)


def _configure_lxd_common() -> None:
    run("sudo snap set lxd daemon.user.group=adm")
    _wait_for_lxd_daemon_responsive()
    _run_lxd_minimal_init()
    # Remove password trust left by older cephtools versions before exposing
    # the LXD API. MAAS enrollment enables it only for the duration of a
    # single registration attempt.
    run(["lxc", "config", "unset", "core.trust_password"], quiet=True)
    run(["lxc", "config", "set", "core.https_address", ":8443"])


def lxd_init_impl(ip, lxdbridge):
    _stop_bind9_for_lxd_setup()
    _wait_for_bind9_shutdown()
    try:
        # Use minimal init so LXD does not auto-create lxdbr0 with dnsmasq
        # enabled before we can disable DNS/DHCP. We create and configure the
        # managed bridges explicitly afterwards.
        _configure_lxd_common()
        ensure_lxd_network(lxdbridge)
        ensure_lxd_default_profile_network(lxdbridge)
        ensure_lxd_network(EXT_LXD_NETWORK)
        time.sleep(2)
    finally:
        _start_bind9_after_lxd_setup()


def lxd_init_vm_impl(
    lxdbridge: str,
    maas_lxdbridge: str,
    maas_lxd_project: str,
) -> None:
    """Initialize host LXD for VM-mode without touching host bind9."""
    _configure_lxd_common()
    ensure_lxd_host_network(lxdbridge)
    ensure_lxd_default_profile_network(lxdbridge)
    ensure_lxd_maas_network(maas_lxdbridge)
    ensure_lxd_maas_project(maas_lxd_project, maas_lxdbridge)
    time.sleep(2)


def lxd_init_lxd_impl(lxdbridge: str) -> None:
    """Initialize host LXD for the LXD-only substrate."""
    _configure_lxd_common()
    ensure_lxd_host_network(lxdbridge)
    ensure_lxd_default_profile_network(lxdbridge)
    ensure_lxd_host_network(EXT_LXD_NETWORK)
    time.sleep(2)


def verify_lxd(lxdbridge):
    info = json.loads(run("lxc query /1.0").stdout)
    if info.get("api_status") != "stable":
        raise RuntimeError("LXD api_status != stable")
    https_addr = run("lxc config get core.https_address").stdout.strip()
    if https_addr != ":8443":
        raise RuntimeError(f"Expected core.https_address ':8443', got '{https_addr}'")
    nets = json.loads(run("lxc query /1.0/networks").stdout)
    if f"/1.0/networks/{lxdbridge}" not in nets:
        raise RuntimeError(f"Network {lxdbridge} not found")
    net = json.loads(run(f"lxc query /1.0/networks/{lxdbridge}").stdout)
    if net.get("managed") is not True:
        raise RuntimeError(f"Network {lxdbridge} is not managed")
    if f"/1.0/networks/{EXT_LXD_NETWORK}" in nets:
        ext_net = json.loads(run(f"lxc query /1.0/networks/{EXT_LXD_NETWORK}").stdout)
        if ext_net.get("managed") is not True:
            raise RuntimeError(f"Network {EXT_LXD_NETWORK} is not managed")


def lxd_warmup():
    """Create a temporary VM to warm up LXD and DNS."""
    click.echo("Warming up LXD with a temporary 24.04 VM...")
    vm_name = WARMUP_VM_NAME
    click.echo(f"Cleaning up any existing instance of {vm_name}...")
    run(f"lxc delete {vm_name} --force", check=False)

    try:
        run(
            f"lxc launch ubuntu:24.04 {vm_name} --vm -c limits.cpu=2 -c limits.memory=1GB </dev/null",
            shell=True,
        )

        deadline = time.monotonic() + 300  # 5 minutes max
        success = False
        attempt = 0
        while time.monotonic() < deadline:
            attempt += 1
            time.sleep(10)
            try:
                run(f"lxc exec {vm_name} -- apt-get update", check=True)
                success = True
                break
            except subprocess.CalledProcessError:
                remaining = max(0, int(deadline - time.monotonic()))
                emit(f"lxd_warmup: attempt {attempt} not ready, {remaining}s remaining")

        if not success:
            click.echo("Warning: Warmup apt-get update timed out.")

    finally:
        run(f"lxc delete {vm_name} --force", check=False)


def _restart_system_resolver() -> None:
    run("sudo resolvectl flush-caches || true", shell=True, check=False)
    run("sudo systemctl restart systemd-resolved || true", shell=True, check=False)


# Bases warmed by juju_warmup(). Juju fetches each base's VM image lazily on
# first deploy; a cold fetch under load can stall provisioning (GH run
# 29607039184). Defaults cover every base the ceph-qa predeployed matrix uses.
JUJU_WARMUP_BASES = ("ubuntu@22.04", "ubuntu@24.04", "ubuntu@26.04")
JUJU_WARMUP_VM_CONSTRAINTS = "virt-type=virtual-machine mem=4G root-disk=32G"
JUJU_WARMUP_TIMEOUT_SECONDS = 600
JUJU_WARMUP_POLL_INTERVAL_SECONDS = 10


def _warmup_model_name(base: str) -> str:
    """Map a base (ubuntu@24.04) to a valid Juju model name (warmup-ubuntu-24-04)."""
    return "warmup-" + base.replace("@", "-").replace(".", "-")


def _destroy_warmup_model(controller: str, model: str) -> None:
    run(
        [
            "juju",
            "destroy-model",
            f"{controller}:{model}",
            "--force",
            "--no-wait",
            "--destroy-storage",
            "--no-prompt",
        ],
        check=False,
    )


def _wait_for_warmup_machine(
    controller: str,
    model: str,
    timeout_seconds: int,
    poll_interval_seconds: int,
) -> None:
    """Poll juju status until machine 0 is started, else raise TimeoutError."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        res = run(
            ["juju", "status", "--model", f"{controller}:{model}", "--format", "json"],
            check=False,
        )
        if res.returncode == 0 and res.stdout:
            try:
                payload = json.loads(res.stdout)
                machines = (
                    payload.get("machines") if isinstance(payload, dict) else None
                )
                m0 = machines.get("0") if isinstance(machines, dict) else None
                if isinstance(m0, dict):
                    # The machine agent is "started" once juju-status reports it.
                    # machine-status only reflects the provider instance state
                    # (e.g. "running") and is not the agent lifecycle. Juju 3.x
                    # status objects use the "current" key (Juju 2.x used
                    # "status"), so check both for compatibility.
                    juju_status = m0.get("juju-status", {})
                    if isinstance(juju_status, dict) and (
                        juju_status.get("current") == "started"
                        or juju_status.get("status") == "started"
                    ):
                        return
            except (json.JSONDecodeError, AttributeError):
                pass  # transient parse issue; keep polling
        time.sleep(poll_interval_seconds)
    raise TimeoutError(f"warmup machine not started within {timeout_seconds}s")


def juju_warmup(
    bases: tuple[str, ...] = JUJU_WARMUP_BASES,
    *,
    controller: str = LXD_CONTROLLER,
    vm_constraints: str = JUJU_WARMUP_VM_CONSTRAINTS,
    timeout_seconds: int = JUJU_WARMUP_TIMEOUT_SECONDS,
    poll_interval_seconds: int = JUJU_WARMUP_POLL_INTERVAL_SECONDS,
) -> None:
    """Pre-cache Juju's per-base VM images by adding then removing a machine.

    Juju fetches each base's VM image lazily on first deploy of that base; a
    cold fetch under load can stall provisioning (GH run 29607039184). Warming
    during ``testenv install`` (reserve time) populates the cache so the first
    matrix job starts warm. Best-effort: any failure emits a warning and moves
    on -- it must never fail the install. LXD substrate only; MAAS commissions
    real machines rather than LXD VMs, so warmup does not apply there.
    """
    for base in bases:
        model = _warmup_model_name(base)
        click.echo(f"Warming up Juju VM image for {base} (model {model})...")
        _destroy_warmup_model(controller, model)  # clean any leftover

        add = run(
            [
                "juju",
                "add-model",
                model,
                "--no-switch",
                "--config",
                f"default-base={base}",
                "--controller",
                controller,
            ],
            check=False,
        )
        if add.returncode != 0:
            click.echo(
                f"Warning: warmup add-model for {base} failed (rc={add.returncode}); "
                f"skipping. Output:\n{add.stdout}"
            )
            continue

        run(
            [
                "juju",
                "set-model-constraints",
                "--model",
                f"{controller}:{model}",
                vm_constraints,
            ],
            check=False,
        )

        try:
            add_machine = run(
                [
                    "juju",
                    "add-machine",
                    "--base",
                    base,
                    "--model",
                    f"{controller}:{model}",
                ],
                check=False,
            )
            if add_machine.returncode != 0:
                click.echo(
                    f"Warning: warmup add-machine for {base} failed "
                    f"(rc={add_machine.returncode}); skipping. Output:\n{add_machine.stdout}"
                )
                continue

            _wait_for_warmup_machine(
                controller, model, timeout_seconds, poll_interval_seconds
            )
            click.echo(f"Warmed up Juju VM image for {base}.")
        except Exception as exc:  # best-effort: never fail install
            click.echo(f"Warning: warmup for {base} did not complete: {exc}")
        finally:
            _destroy_warmup_model(controller, model)


def _lxd_instance_exists(name: str) -> bool:
    result = run(["lxc", "info", name], check=False, quiet=True)
    return result.returncode == 0


def _run_in_lxd_instance(
    name: str,
    command: str | list[str],
    *,
    check: bool = True,
    quiet: bool = False,
) -> subprocess.CompletedProcess[str]:
    if isinstance(command, str):
        return run(
            ["lxc", "exec", name, "--", "bash", "-lc", command],
            check=check,
            quiet=quiet,
        )
    return run(["lxc", "exec", name, "--", *command], check=check, quiet=quiet)


def _lxd_network_interface(network_name: str):
    net = json.loads(run(f"lxc query /1.0/networks/{network_name}").stdout)
    config = net.get("config") or {}
    ipv4_address = config.get("ipv4.address")
    if not ipv4_address or str(ipv4_address).lower() == "none":
        raise RuntimeError(f"LXD network {network_name} lacks an IPv4 address")
    return ip_interface(str(ipv4_address))


def derive_maas_vm_ip(network_name: str, configured_ip: str | None = None) -> str:
    if configured_ip:
        return configured_ip
    bridge_ip = _lxd_network_interface(network_name)
    for host in bridge_ip.network.hosts():
        if host != bridge_ip.ip:
            return str(host)
    raise RuntimeError(f"Unable to choose a MAAS VM IP on {bridge_ip.network}")


def render_maas_vm_network_config(network_name: str, maas_vm_ip: str) -> str:
    bridge_ip = _lxd_network_interface(network_name)
    address = ip_interface(f"{maas_vm_ip}/{bridge_ip.network.prefixlen}")
    return (
        "version: 2\n"
        "ethernets:\n"
        "  maas0:\n"
        "    match:\n"
        "      name: en*\n"
        "    set-name: eth0\n"
        "    dhcp4: false\n"
        f"    addresses: [{address.with_prefixlen}]\n"
        f"    routes: [{{to: default, via: {bridge_ip.ip}}}]\n"
        "    nameservers: {addresses: [1.1.1.1, 8.8.8.8]}\n"
    )


def render_maas_vm_bootstrap_script(
    maas_url: str,
    admin: str,
    admin_pw: str,
    admin_mail: str,
    maas_version: str,
) -> str:
    channel = f"{maas_version}/stable" if maas_version else "latest/stable"
    return f"""#!/usr/bin/env bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
export PATH="/snap/bin:$PATH"
snap wait system seed.loaded
if ! snap list maas-test-db >/dev/null 2>&1; then
    snap install maas-test-db
fi
if ! snap list maas >/dev/null 2>&1; then
    snap install maas --channel={shlex.quote(channel)}
fi
if ! maas status >/dev/null 2>&1; then
    maas init region+rack --database-uri maas-test-db:/// --maas-url {shlex.quote(maas_url)}
fi
maas_ready=false
for _ in $(seq 1 120); do
    status="$(maas status || true)"
    if echo "$status" | grep -Eq '^regiond[[:space:]]+enabled[[:space:]]+active' \
       && echo "$status" | grep -Eq '^rackd[[:space:]]+enabled[[:space:]]+active' \
       && echo "$status" | grep -Eq '^apiserver[[:space:]]+enabled[[:space:]]+active'; then
        maas_ready=true
        break
    fi
    sleep 5
done
maas status
if [ "$maas_ready" != true ]; then
    exit 1
fi
if ! maas apikey --username {shlex.quote(admin)} >/dev/null 2>&1; then
    maas createadmin --username {shlex.quote(admin)} --password {shlex.quote(admin_pw)} --email {shlex.quote(admin_mail)}
fi
api_key="$(maas apikey --username {shlex.quote(admin)})"
maas login {shlex.quote(admin)} {shlex.quote(maas_url)} "$api_key" || true
maas {shlex.quote(admin)} maas set-config name=upstream_dns value=1.1.1.1 || true
"""


def ensure_maas_vm(
    vm_name: str,
    image: str,
    cpus: int,
    memory: str,
    disk: str,
    network_name: str,
    maas_vm_ip: str,
) -> None:
    network_config = render_maas_vm_network_config(network_name, maas_vm_ip)
    if not _lxd_instance_exists(vm_name):
        run(
            [
                "lxc",
                "init",
                image,
                vm_name,
                "--vm",
                "-c",
                f"limits.cpu={cpus}",
                "-c",
                f"limits.memory={memory}",
                "-c",
                "security.secureboot=false",
            ]
        )
        run(["lxc", "config", "device", "override", vm_name, "root", f"size={disk}"])
        run(
            [
                "lxc",
                "config",
                "device",
                "override",
                vm_name,
                "eth0",
                f"network={network_name}",
                "name=eth0",
            ]
        )
        run(["lxc", "config", "set", vm_name, "user.network-config", network_config])
        run(["lxc", "start", vm_name])
    else:
        run(["lxc", "config", "set", vm_name, "user.network-config", network_config])
        state = run(["lxc", "query", f"/1.0/instances/{vm_name}/state"], quiet=True)
        if json.loads(state.stdout or "{}").get("status") != "Running":
            run(["lxc", "start", vm_name])


def wait_for_lxd_vm_cloud_init(vm_name: str, *, timeout: int = 900) -> None:
    deadline = time.monotonic() + timeout
    last_error = ""
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        result = _run_in_lxd_instance(
            vm_name,
            "cloud-init status --wait || cloud-init status --long",
            check=False,
            quiet=True,
        )
        if result.returncode == 0:
            return
        last_error = _format_process_error(result)
        remaining = max(0, int(deadline - time.monotonic()))
        emit(
            f"wait_for_cloud_init[{vm_name}]: attempt {attempt} not done "
            f"(rc={result.returncode}), {remaining}s remaining"
        )
        time.sleep(10)
    raise click.ClickException(
        f"Timed out waiting for cloud-init in {vm_name}: {last_error}"
    )


def maas_vm_init_impl(
    vm_name: str,
    image: str,
    cpus: int,
    memory: str,
    disk: str,
    network_name: str,
    maas_vm_ip: str,
    maas_url: str,
    admin: str,
    admin_pw: str,
    admin_mail: str,
    maas_version: str,
) -> str:
    with operation("3/7", "maas-vm-init:ensure_maas_vm"):
        ensure_maas_vm(vm_name, image, cpus, memory, disk, network_name, maas_vm_ip)
    with operation("3/7", "maas-vm-init:wait_for_cloud_init"):
        wait_for_lxd_vm_cloud_init(vm_name)
    with operation("3/7", "maas-vm-init:network_setup"):
        _run_in_lxd_instance(
            vm_name,
            "netplan apply && resolvectl dns eth0 1.1.1.1 8.8.8.8 && resolvectl domain eth0 '~.'",
        )
    with operation("3/7", "maas-vm-init:bootstrap_maas"):
        script = render_maas_vm_bootstrap_script(
            maas_url, admin, admin_pw, admin_mail, maas_version
        )
        local_script = Path("/tmp") / f"{vm_name}-maas-bootstrap.sh"
        local_script.write_text(script)
        os.chmod(local_script, 0o700)
        _run_in_lxd_instance(vm_name, ["rm", "-f", MAAS_VM_BOOTSTRAP_SCRIPT])
        run(
            [
                "lxc",
                "file",
                "push",
                "--uid",
                "0",
                "--gid",
                "0",
                "--mode",
                "700",
                str(local_script),
                f"{vm_name}{MAAS_VM_BOOTSTRAP_SCRIPT}",
            ]
        )
        _run_in_lxd_instance(vm_name, [MAAS_VM_BOOTSTRAP_SCRIPT])
    with operation("3/7", "maas-vm-init:api_key"):
        api_key = _run_in_lxd_instance(
            vm_name,
            f"maas apikey --username {shlex.quote(admin)}",
            quiet=True,
        ).stdout.strip()
    return api_key


def _resolve_hostname(hostname: str) -> bool:
    try:
        socket.getaddrinfo(hostname, 443, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return False
    return True


def dns_preflight(
    *,
    hosts: tuple[str, ...] = DNS_PRECHECK_HOSTS,
    timeout: int = DNS_PRECHECK_TIMEOUT_SECONDS,
    interval: int = DNS_PRECHECK_INTERVAL_SECONDS,
) -> None:
    click.echo("Restarting resolver and running DNS preflight checks...")
    _restart_system_resolver()

    unresolved = set(hosts)
    deadline = time.monotonic() + timeout
    attempt = 0

    while unresolved and time.monotonic() < deadline:
        attempt += 1
        for host in list(unresolved):
            if _resolve_hostname(host):
                unresolved.remove(host)

        if unresolved:
            click.echo(
                "DNS preflight attempt "
                f"{attempt} pending hosts: {', '.join(sorted(unresolved))}"
            )
            time.sleep(interval)

    if unresolved:
        unresolved_hosts = ", ".join(sorted(unresolved))
        resolver_status = run(
            "resolvectl status || true", shell=True, check=False, quiet=True
        )
        if resolver_status.stdout:
            click.echo(resolver_status.stdout)
        raise click.ClickException(
            "DNS preflight failed; unresolved hosts: "
            f"{unresolved_hosts}. Check resolver and network egress."
        )

    click.echo("DNS preflight checks passed.")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _maas_local_config() -> dict[str, object]:
    result = run(["sudo", "maas-region", "local_config_get", "--json"], quiet=True)
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise click.ClickException(
            "Failed to parse MAAS local configuration as JSON."
        ) from exc
    if not isinstance(payload, dict):  # pragma: no cover - defensive
        raise click.ClickException("MAAS local configuration has unexpected format.")
    return payload


def _maas_is_initialized() -> bool:
    return bool(_maas_local_config())


def _postgres_role_exists(role: str) -> bool:
    result = run(
        [
            "sudo",
            "-u",
            "postgres",
            "psql",
            "-tAc",
            f"SELECT 1 FROM pg_roles WHERE rolname={_sql_literal(role)}",
        ],
        quiet=True,
    )
    return result.stdout.strip() == "1"


def _postgres_database_exists(name: str) -> bool:
    result = run(
        [
            "sudo",
            "-u",
            "postgres",
            "psql",
            "-tAc",
            f"SELECT 1 FROM pg_database WHERE datname={_sql_literal(name)}",
        ],
        quiet=True,
    )
    return result.stdout.strip() == "1"


def _disable_systemd_timesyncd() -> None:
    unit = run(
        ["systemctl", "list-unit-files", "systemd-timesyncd.service", "--no-legend"],
        check=False,
        quiet=True,
    )
    if "systemd-timesyncd.service" not in (unit.stdout or ""):
        return
    run(["sudo", "systemctl", "disable", "--now", "systemd-timesyncd"])


def _ensure_maas_postgres(password: str) -> None:
    _disable_systemd_timesyncd()

    role = _sql_identifier(MAAS_DB_USER)
    password_sql = _sql_literal(password)
    if _postgres_role_exists(MAAS_DB_USER):
        run(
            [
                "sudo",
                "-u",
                "postgres",
                "psql",
                "-c",
                f"ALTER USER {role} WITH ENCRYPTED PASSWORD {password_sql}",
            ]
        )
    else:
        run(
            [
                "sudo",
                "-u",
                "postgres",
                "psql",
                "-c",
                f"CREATE USER {role} WITH ENCRYPTED PASSWORD {password_sql}",
            ]
        )

    if not _postgres_database_exists(MAAS_DB_NAME):
        run(["sudo", "-u", "postgres", "createdb", "-O", MAAS_DB_USER, MAAS_DB_NAME])


def _configure_maas_region(maas_url: str, db_password: str) -> None:
    run(
        [
            "sudo",
            "maas-region",
            "local_config_set",
            "--database-host",
            MAAS_DB_HOST,
            "--database-port",
            MAAS_DB_PORT,
            "--database-name",
            MAAS_DB_NAME,
            "--database-user",
            MAAS_DB_USER,
            "--database-pass",
            db_password,
            "--maas-url",
            maas_url,
        ]
    )
    run(["sudo", "maas-region", "dbupgrade"])
    for service in (
        "maas-regiond",
        "maas-rackd",
        "maas-apiserver",
        "maas-http",
        "maas-temporal",
        "maas-temporal-worker",
    ):
        run(["sudo", "systemctl", "restart", service], check=False)


def _run_maas_cli(
    command: str,
    *,
    maas_vm_name: str | None = None,
    check: bool = True,
    quiet: bool = False,
    shell: bool = False,
) -> subprocess.CompletedProcess[str]:
    if maas_vm_name:
        return _run_in_lxd_instance(maas_vm_name, command, check=check, quiet=quiet)
    return run(command, check=check, quiet=quiet, shell=shell)


def _run_maas_apikey(
    admin: str,
    *,
    maas_vm_name: str | None = None,
    check: bool = True,
    quiet: bool = False,
) -> subprocess.CompletedProcess[str]:
    command = f"maas apikey --username {shlex.quote(admin)}"
    if maas_vm_name:
        return _run_in_lxd_instance(maas_vm_name, command, check=check, quiet=quiet)
    return run(f"sudo {command}", check=check, quiet=quiet)


def _maas_admin_exists(admin: str) -> bool:
    result = _run_maas_apikey(admin, check=False, quiet=True)
    return result.returncode == 0 and bool((result.stdout or "").strip())


def _ensure_maas_auth_ready() -> None:
    run(["sudo", "maas-region", "configauth", "--json"], quiet=True)


def maas_init_impl(maas_url, admin, admin_pw, admin_mail):
    with operation("3/7", "maas-init:_ensure_maas_postgres"):
        already_initialized = _maas_is_initialized()
        if already_initialized:
            emit(
                "MAAS already configured; ensuring database settings, "
                "services, and admin user."
            )
        _ensure_maas_postgres(admin_pw)

    with operation("3/7", "maas-init:_configure_maas_region"):
        _configure_maas_region(maas_url, admin_pw)

    with operation("3/7", "maas-init:_ensure_maas_auth_ready"):
        _ensure_maas_auth_ready()

    with operation("3/7", "maas-init:createadmin"):
        if not _maas_admin_exists(admin):
            try:
                run(
                    [
                        "sudo",
                        "maas",
                        "createadmin",
                        "--username",
                        admin,
                        "--password",
                        admin_pw,
                        "--email",
                        admin_mail,
                    ]
                )
            except subprocess.CalledProcessError as e:
                print((e.stderr or "").strip())

    time.sleep(10)


def maas_api_key(admin, *, maas_vm_name: str | None = None) -> str:
    out = _run_maas_apikey(admin, maas_vm_name=maas_vm_name)
    return out.stdout.strip()


def maas_login(maas_url, admin, api_key, *, maas_vm_name: str | None = None):
    _run_maas_cli(
        f"maas login {shlex.quote(admin)} {shlex.quote(maas_url)} {shlex.quote(api_key)}",
        maas_vm_name=maas_vm_name,
    )


def verify_maas(admin, *, maas_vm_name: str | None = None):
    if maas_vm_name:
        status = _run_in_lxd_instance(
            maas_vm_name, "maas status", check=False, quiet=True
        )
        if status.returncode != 0:
            raise RuntimeError(
                f"MAAS snap services not ready: {_format_process_error(status)}"
            )
        service_states = {}
        for line in status.stdout.splitlines():
            columns = line.split()
            if len(columns) >= 3:
                service_states[columns[0]] = columns[1:3]
        for service in ("regiond", "rackd", "apiserver"):
            if service_states.get(service) != ["enabled", "active"]:
                raise RuntimeError(f"MAAS snap service {service} is not active")
    else:
        regiond = run(
            ["sudo", "systemctl", "is-active", "--quiet", "maas-regiond"],
            check=False,
            quiet=True,
        )
        rackd = run(
            ["sudo", "systemctl", "is-active", "--quiet", "maas-rackd"],
            check=False,
            quiet=True,
        )
        if regiond.returncode != 0 or rackd.returncode != 0:
            raise RuntimeError(
                "MAAS services not running (maas-regiond/maas-rackd must be active)"
            )
    _ = _run_maas_cli(
        f"maas {shlex.quote(admin)} boot-resources read",
        maas_vm_name=maas_vm_name,
    ).stdout


def register_lxd_vmhost_impl(
    admin,
    vmhost,
    ip,
    *,
    project: str = "default",
    maas_vm_name: str | None = None,
):
    try:
        if maas_vm_name is None:
            existing_id = _get_lxd_vm_host_id(admin, vmhost)
        else:
            existing_id = _get_lxd_vm_host_id(admin, vmhost, maas_vm_name=maas_vm_name)
    except VMHostNotFound:
        existing_id = None
    if existing_id is not None:
        click.echo(
            f"VM host '{vmhost}' already registered in MAAS (id {existing_id}); skipping create."
        )
        return

    trust_password = secrets.token_urlsafe(32)
    try:
        try:
            trust_result = run(
                ["lxc", "config", "set", "core.trust_password", trust_password],
                check=False,
                quiet=True,
            )
        except subprocess.CalledProcessError:
            raise click.ClickException(
                "Failed to enable temporary LXD password trust."
            ) from None
        if trust_result.returncode != 0:
            raise click.ClickException("Failed to enable temporary LXD password trust.")

        try:
            enrollment_result = _run_maas_cli(
                " ".join(
                    [
                        f'maas "{admin}" vm-hosts create type=lxd',
                        f'name="{vmhost}"',
                        f'project="{project}"',
                        f'power_address="https://{ip}:8443"',
                        f'password="{trust_password}"',
                    ]
                ),
                check=False,
                shell=True,
                quiet=True,
                maas_vm_name=maas_vm_name,
            )
        except subprocess.CalledProcessError:
            raise click.ClickException("Failed to register the LXD VM host.") from None
        if enrollment_result.returncode != 0:
            detail = _format_process_error(enrollment_result).replace(
                trust_password, "<redacted>"
            )
            raise click.ClickException(f"Failed to register the LXD VM host: {detail}")
    except BaseException as enrollment_error:
        try:
            run(
                ["lxc", "config", "unset", "core.trust_password"],
                quiet=True,
            )
        except BaseException:
            enrollment_error.add_note(
                "Additionally failed to disable temporary LXD password trust."
            )
        raise
    else:
        run(
            ["lxc", "config", "unset", "core.trust_password"],
            quiet=True,
        )


def extract_arches(resources):
    """Return the syncd arches from MAAS boot-resources JSON."""

    ready_arches: set[str] = set()
    for item in resources:
        if not isinstance(item, dict):
            continue

        if item.get("type") != "Synced":
            continue

        architecture = item.get("architecture")
        if not architecture:
            continue

        architecture_str = str(architecture)
        ready_arches.add(architecture_str)

        base_arch, _, _subarch = architecture_str.partition("/")
        if not base_arch:
            continue

        subarches = item.get("subarches")
        if isinstance(subarches, str):
            for subarch in subarches.split(","):
                subarch = subarch.strip()
                if subarch:
                    ready_arches.add(f"{base_arch}/{subarch}")

    return ready_arches


def import_boot_resources(admin, *, maas_vm_name: str | None = None):
    """Import images, wait for them to become available."""
    _run_maas_cli(f'maas "{admin}" boot-resources import', maas_vm_name=maas_vm_name)
    time.sleep(15)
    # read boot and loop until we have the required architecture
    for attempt in range(120):
        out = _run_maas_cli(
            f"maas {shlex.quote(admin)} boot-resources read",
            maas_vm_name=maas_vm_name,
        ).stdout
        resources = json.loads(out)
        arches = extract_arches(resources)
        if REQUIRED_BOOT_ARCHITECTURE in arches:
            click.echo(
                f"Found {REQUIRED_BOOT_ARCHITECTURE}, waiting for it to stabilize..."
            )
            time.sleep(30)
            # Final check to ensure it didn't disappear (e.g. failed download)
            out = _run_maas_cli(
                f"maas {shlex.quote(admin)} boot-resources read",
                maas_vm_name=maas_vm_name,
            ).stdout
            resources = json.loads(out)
            arches = extract_arches(resources)
            if REQUIRED_BOOT_ARCHITECTURE in arches:
                return
            raise Exception(
                f"Boot resource {REQUIRED_BOOT_ARCHITECTURE} disappeared after import!"
            )
        remaining = max(0, (119 - attempt) * 6)
        emit(
            f"import_boot_resources: attempt {attempt + 1}/120, "
            f"synced_arches=[{', '.join(sorted(arches)) or 'none'}], "
            f"~{remaining}s remaining"
        )
        time.sleep(6)
    raise Exception("Failed to import boot resources")


def route_info(lxdbridge):
    out = run(f"ip -j r s dev {lxdbridge}")
    routes = json.loads(out.stdout)

    for route in routes:
        dst = route.get("dst")
        prefsrc = route.get("prefsrc")

        if dst and "/" in dst and prefsrc:
            return dst, prefsrc

    raise RuntimeError(f"could not derive CIDR or gateway from routes: {routes}")


def lxd_network_cidr_and_gateway(network_name: str) -> tuple[str, str]:
    iface = _lxd_network_interface(network_name)
    cidr = iface.network.with_prefixlen
    return str(cidr), str(iface.ip)


def maas_subnet_ids(admin, cidr, *, maas_vm_name: str | None = None):
    subnets = json.loads(
        _run_maas_cli(
            f"maas {shlex.quote(admin)} subnets read",
            maas_vm_name=maas_vm_name,
        ).stdout
    )
    sid = next((s["id"] for s in subnets if s.get("cidr") == cidr), None)
    if sid is None:
        raise RuntimeError(f"MAAS subnet for {cidr} not found")
    subnet = json.loads(
        _run_maas_cli(
            f"maas {shlex.quote(admin)} subnet read {sid}",
            maas_vm_name=maas_vm_name,
        ).stdout
    )
    fabric_id = subnet["vlan"]["fabric_id"]
    vlan_id = subnet["vlan"]["vid"]
    racks = json.loads(
        _run_maas_cli(
            f"maas {shlex.quote(admin)} rack-controllers read",
            maas_vm_name=maas_vm_name,
        ).stdout
    )
    rack_sysid = racks[0]["system_id"]
    return sid, fabric_id, vlan_id, rack_sysid


def update_subnet_gateway(admin, subnet_id, gw, *, maas_vm_name: str | None = None):
    _run_maas_cli(
        f"maas {shlex.quote(admin)} subnet update {subnet_id} gateway_ip={gw}",
        maas_vm_name=maas_vm_name,
    )


def create_dynamic_iprange(
    admin,
    subnet_id,
    cidr,
    *,
    reserved_ips: set[str] | None = None,
    maas_vm_name: str | None = None,
):
    hosts = list(ip_network(cidr).hosts())
    if len(hosts) < 80:
        raise RuntimeError("subnet too small for 80 hosts")
    excluded = set(reserved_ips or set())
    candidates = [str(host) for host in hosts if str(host) not in excluded]
    start_ip, end_ip = candidates[-80], candidates[-1]
    _run_maas_cli(
        f"maas {shlex.quote(admin)} ipranges create type=dynamic subnet={subnet_id} "
        f'start_ip="{start_ip}" end_ip="{end_ip}" || true',
        shell=True,
        maas_vm_name=maas_vm_name,
    )
    time.sleep(12)  # wait for MAAS to process
    return start_ip, end_ip


def enable_vlan_dhcp(
    admin, fabric_id, vlan_id, rack_sysid, *, maas_vm_name: str | None = None
):
    _run_maas_cli(
        f"maas {shlex.quote(admin)} vlan update {fabric_id} {vlan_id} "
        f"dhcp_on=true primary_rack={rack_sysid}",
        maas_vm_name=maas_vm_name,
    )


def create_space(admin, space_name, *, maas_vm_name: str | None = None):
    spaces = json.loads(
        _run_maas_cli(
            f"maas {shlex.quote(admin)} spaces read",
            maas_vm_name=maas_vm_name,
        ).stdout
    )
    space_id = next((s["id"] for s in spaces if s.get("name") == space_name), None)
    if space_id is not None:
        return space_id

    _run_maas_cli(
        f'maas {shlex.quote(admin)} spaces create name="{space_name}"',
        maas_vm_name=maas_vm_name,
    )
    spaces = json.loads(
        _run_maas_cli(
            f"maas {shlex.quote(admin)} spaces read",
            maas_vm_name=maas_vm_name,
        ).stdout
    )
    space_id = next((s["id"] for s in spaces if s.get("name") == space_name), None)
    if space_id is None:
        raise RuntimeError(f"MAAS space '{space_name}' not found after creation")
    return space_id


def assign_space_to_vlan(
    admin, fabric_id, vlan_id, space_id, *, maas_vm_name: str | None = None
):
    _run_maas_cli(
        f"maas {shlex.quote(admin)} vlan update {fabric_id} {vlan_id} space={space_id}",
        maas_vm_name=maas_vm_name,
    )


class VMHostNotFound(click.ClickException):
    """Raised when a named MAAS VM host is genuinely absent."""


def _get_lxd_vm_host_id(
    admin: str, vmhost: str, *, maas_vm_name: str | None = None
) -> str:
    result = _run_maas_cli(
        f"maas {shlex.quote(admin)} vm-hosts read",
        maas_vm_name=maas_vm_name,
    )
    try:
        hosts = json.loads(result.stdout)
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise click.ClickException(
            "Failed to parse MAAS vm-hosts output as JSON."
        ) from exc
    for host in hosts:
        if host.get("name") == vmhost:
            host_id = host.get("id") or host.get("system_id")
            if host_id is None:
                break
            return str(host_id)
    raise VMHostNotFound(f"VM host '{vmhost}' not found in MAAS vm-hosts output.")


def _get_vm_host_architectures(
    admin: str, vmhost: str, *, maas_vm_name: str | None = None
) -> list[str]:
    result = _run_maas_cli(
        f"maas {shlex.quote(admin)} vm-hosts read",
        maas_vm_name=maas_vm_name,
    )
    try:
        hosts = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise click.ClickException(
            "Failed to parse MAAS vm-hosts output as JSON."
        ) from exc

    for host in hosts:
        if host.get("name") != vmhost:
            continue
        architectures = host.get("architectures") or []
        if isinstance(architectures, list):
            return [str(arch) for arch in architectures if arch]
        return []

    raise click.ClickException(f"VM host '{vmhost}' not found in MAAS vm-hosts output.")


def _wait_for_vm_host_architecture(
    admin: str,
    vmhost: str,
    architecture: str,
    *,
    maas_vm_name: str | None = None,
    timeout: int = 600,
    interval: int = 6,
) -> None:
    """Poll until the MAAS VM host reports the required architecture."""
    deadline = time.monotonic() + timeout
    last_seen: list[str] = []
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        if maas_vm_name is None:
            architectures = _get_vm_host_architectures(admin, vmhost)
        else:
            architectures = _get_vm_host_architectures(
                admin, vmhost, maas_vm_name=maas_vm_name
            )
        if architecture in architectures:
            return
        last_seen = architectures
        remaining = max(0, int(deadline - time.monotonic()))
        emit(
            f"wait_for_vm_host_arch: attempt {attempt}, "
            f"seen=[{', '.join(last_seen) or 'none'}], {remaining}s remaining"
        )
        time.sleep(interval)

    seen_msg = ", ".join(last_seen) if last_seen else "none"
    raise click.ClickException(
        f"Timed out waiting for MAAS VM host '{vmhost}' to report architecture "
        f"'{architecture}'. Last seen architectures: {seen_msg}."
    )


def write_cloud_yaml(ip):
    cloud_path = get_state_file("cloud.yaml")
    cloud_path.write_text(
        "clouds:\n"
        "  maas-cloud:\n"
        "    type: maas\n"
        "    auth-types: [oauth1]\n"
        f"    endpoint: http://{ip}:5240/MAAS\n"
    )
    return cloud_path


def write_cred_yaml(api_key):
    cred_path = get_state_file("cred.yaml")
    cred_path.write_text(
        "credentials:\n"
        "  maas-cloud:\n"
        "    admin:\n"
        "      auth-type: oauth1\n"
        f"      maas-oauth: {api_key}\n"
    )
    return cred_path


def _juju_cloud_exists(juju: jubilant.Juju, cloud_name: str) -> bool:
    clouds_output = juju.cli(
        "clouds",
        "--client",
        "--format",
        "json",
        include_model=False,
    )
    payload = json.loads(clouds_output or "{}")
    clouds_section = payload.get("clouds")
    if isinstance(clouds_section, dict):
        return cloud_name in clouds_section
    if isinstance(payload, dict):
        return cloud_name in payload
    return False


def _juju_credential_exists(
    juju: jubilant.Juju, cloud_name: str, credential_name: str
) -> bool:
    creds_output = juju.cli(
        "credentials",
        "--client",
        "--format",
        "json",
        include_model=False,
    )
    payload = json.loads(creds_output or "{}")
    credentials = payload.get("credentials")
    if not isinstance(credentials, dict):
        return False
    cloud_credentials = credentials.get(cloud_name)
    if not isinstance(cloud_credentials, dict):
        return False
    return credential_name in cloud_credentials


def _juju_controller_exists(juju: jubilant.Juju, controller_name: str) -> bool:
    controllers_output = juju.cli(
        "controllers",
        "--format",
        "json",
        include_model=False,
    )
    payload = json.loads(controllers_output or "{}")
    controllers = payload.get("controllers")
    return isinstance(controllers, dict) and controller_name in controllers


def _wait_for_controller_ready(juju: jubilant.Juju) -> None:
    time.sleep(10)
    for attempt in range(20):
        controllers_output = juju.cli(
            "controllers",
            "--format",
            "json",
            include_model=False,
        )
        payload = json.loads(controllers_output or "{}")
        total_ctrl_machines = sum(
            controller.get("controller-machines", {}).get("Total", 0)
            for controller in payload.get("controllers", {}).values()
            if isinstance(controller, dict)
        )
        if total_ctrl_machines > 0:
            return
        remaining = max(0, (19 - attempt) * 6)
        emit(
            f"wait_for_controller_ready: attempt {attempt + 1}/20, "
            f"ctrl_machines=0, ~{remaining}s remaining"
        )
        time.sleep(6)

    raise click.ClickException("juju controller machines not ready after timeout")


def juju_onboard(substrate: str = SUBSTRATE_MAAS_HOST) -> bool:
    juju = jubilant.Juju()
    cloud_name = _cloud_name(substrate)
    controller_name = _controller_name(substrate)

    if _is_maas_substrate(substrate):
        cloud_path = get_state_file("cloud.yaml")
        cred_path = get_state_file("cred.yaml")
        if not _juju_cloud_exists(juju, cloud_name):
            click.echo(f"Registering Juju cloud '{cloud_name}'.")
            try:
                juju.cli(
                    "add-cloud",
                    cloud_name,
                    str(cloud_path),
                    "--client",
                    include_model=False,
                )
            except jubilant.CLIError as exc:
                if not _is_already_exists_error(exc):
                    raise
                click.echo("Juju reports cloud already exists; continuing.")
        else:
            click.echo(f"Juju cloud '{cloud_name}' already registered; skipping.")

        if not _juju_credential_exists(juju, cloud_name, "admin"):
            click.echo(f"Adding Juju credential 'admin' for cloud '{cloud_name}'.")
            try:
                juju.cli(
                    "add-credential",
                    cloud_name,
                    "-f",
                    str(cred_path),
                    "--client",
                    include_model=False,
                )
            except jubilant.CLIError as exc:
                if not _is_already_exists_error(exc):
                    raise
                click.echo("Juju reports credential already exists; continuing.")
        else:
            click.echo("Juju credential 'admin' already present; skipping.")

        bootstrap_constraints: dict[str, str] = {"spaces": JUJU_SPACE_NAME}
        bootstrap_config: dict[str, str] | None = {"juju-mgmt-space": JUJU_SPACE_NAME}
    else:
        bootstrap_constraints = {"virt-type": "virtual-machine"}
        bootstrap_config = None

    time.sleep(2)

    bootstrapped = False
    if not _juju_controller_exists(juju, controller_name):
        click.echo(f"Bootstrapping Juju controller '{controller_name}'.")
        bootstrap_kwargs: dict[str, object] = {
            "bootstrap_constraints": bootstrap_constraints
        }
        if bootstrap_config is not None:
            bootstrap_kwargs["config"] = bootstrap_config
        juju.bootstrap(cloud_name, controller_name, **bootstrap_kwargs)
        bootstrapped = True
    else:
        click.echo(
            f"Juju controller '{controller_name}' already exists; skipping bootstrap."
        )

    click.echo(f"Switching to Juju controller '{controller_name}'.")
    juju.cli("switch", controller_name, include_model=False)

    if bootstrapped:
        click.echo("Waiting for controller machines to report ready status.")
        _wait_for_controller_ready(juju)

    return bootstrapped


def _is_already_exists_error(exc: jubilant.CLIError) -> bool:
    message = _format_juju_error(exc).lower()
    return "already exists" in message


def _juju_model_ref(controller: str, model: str) -> str:
    return f"{controller}:{model}"


def write_lxd_network_yaml(
    *,
    lxdbridge: str,
    lxd_cidr: str,
    lxd_gateway: str,
    ext_cidr: str,
    ext_gateway: str,
) -> Path:
    network_yaml = "\n".join(
        [
            "network:",
            f"  substrate: {SUBSTRATE_LXD}",
            f"  bridge: {lxdbridge}",
            f"  cidr: {lxd_cidr}",
            f"  gateway: {lxd_gateway}",
            "  external:",
            f"    bridge: {EXT_LXD_NETWORK}",
            f"    cidr: {ext_cidr}",
            f"    gateway: {ext_gateway}",
            f"    space: {EXTERNAL_SPACE_NAME}",
            "",
        ]
    )
    network_path = get_state_file("network.yaml")
    network_path.write_text(network_yaml)
    return network_path


def _juju_subnet_space(model_ref: str, cidr: str) -> str | None:
    """Return the Juju space a subnet CIDR is currently assigned to, if any."""
    result = run(
        ["juju", "spaces", "--format", "json", "-m", model_ref],
        quiet=True,
    )
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise click.ClickException("Failed to parse Juju spaces output.") from exc
    spaces = payload.get("spaces")
    if not isinstance(spaces, list):
        return None
    for space in spaces:
        if not isinstance(space, dict):
            continue
        subnets = space.get("subnets")
        if isinstance(subnets, dict) and cidr in subnets:
            name = space.get("name")
            return str(name) if name is not None else None
    return None


def configure_lxd_juju_network(*, controller: str, model: str, lxdbridge: str) -> None:
    lxd_cidr, lxd_gateway = lxd_network_cidr_and_gateway(lxdbridge)
    ext_cidr, ext_gateway = lxd_network_cidr_and_gateway(EXT_LXD_NETWORK)
    model_ref = _juju_model_ref(controller, model)
    run(["juju", "reload-spaces", "-m", model_ref])
    run(["juju", "add-space", EXTERNAL_SPACE_NAME, "-m", model_ref], check=False)
    # move-to-space is not a no-op when the subnet is already in the target
    # space, so only move it when it is not already assigned there. This keeps
    # configure-network idempotent across repeated install runs.
    if _juju_subnet_space(model_ref, ext_cidr) != EXTERNAL_SPACE_NAME:
        run(["juju", "move-to-space", EXTERNAL_SPACE_NAME, ext_cidr, "-m", model_ref])
    write_lxd_network_yaml(
        lxdbridge=lxdbridge,
        lxd_cidr=lxd_cidr,
        lxd_gateway=lxd_gateway,
        ext_cidr=ext_cidr,
        ext_gateway=ext_gateway,
    )


def _machine_sort_key(machine_id: str) -> tuple[int, str]:
    return (0, f"{int(machine_id):012d}") if machine_id.isdigit() else (1, machine_id)


def _juju_machine_instance_ids(controller: str, model: str) -> dict[str, str]:
    result = run(
        [
            "juju",
            "machines",
            "--format",
            "json",
            "-m",
            _juju_model_ref(controller, model),
        ],
        quiet=True,
    )
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise click.ClickException("Failed to parse Juju machines output.") from exc
    machines = payload.get("machines")
    if not isinstance(machines, dict):
        return {}

    instance_ids: dict[str, str] = {}
    for machine_id, machine in machines.items():
        if not isinstance(machine, dict):
            continue
        instance_id = machine.get("instance-id")
        if isinstance(instance_id, str) and instance_id and instance_id != "pending":
            instance_ids[str(machine_id)] = instance_id
    return instance_ids


def _wait_for_lxd_juju_machines(
    controller: str,
    model: str,
    desired_count: int,
    *,
    timeout: int = 900,
    interval: int = 10,
) -> dict[str, str]:
    deadline = time.monotonic() + timeout
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        instance_ids = _juju_machine_instance_ids(controller, model)
        if len(instance_ids) >= desired_count:
            selected = sorted(instance_ids, key=_machine_sort_key)[:desired_count]
            return {machine_id: instance_ids[machine_id] for machine_id in selected}
        remaining = max(0, int(deadline - time.monotonic()))
        emit(
            f"wait_for_lxd_juju_machines: attempt {attempt}, "
            f"ready={len(instance_ids)}/{desired_count}, {remaining}s remaining"
        )
        time.sleep(interval)
    raise click.ClickException(
        f"Timed out waiting for {desired_count} Juju LXD machines in model {model}."
    )


def _lxd_storage_volume_exists(pool: str, volume: str) -> bool:
    result = run(
        ["lxc", "storage", "volume", "show", pool, f"custom/{volume}"],
        check=False,
        quiet=True,
    )
    return result.returncode == 0


def _lxd_instance_device_names(instance: str) -> set[str]:
    result = run(["lxc", "query", f"/1.0/instances/{instance}"], quiet=True)
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise click.ClickException(
            f"Failed to parse LXD instance {instance} JSON."
        ) from exc
    devices = payload.get("devices")
    if not isinstance(devices, dict):
        return set()
    return {str(name) for name in devices}


def _lxd_osd_volume_name(machine_id: str, disk_index: int) -> str:
    return f"{CEPHTOOLS_MODEL}-{machine_id}-osd-{disk_index}"


def _is_lxd_osd_volume_name(name: str) -> bool:
    pattern = rf"{re.escape(CEPHTOOLS_MODEL)}-[0-9]+-osd-[0-9]+"
    return re.fullmatch(pattern, name) is not None


def _attach_lxd_osd_volumes(
    machine_to_instance: dict[str, str], *, disk_count: int, disk_size: int
) -> None:
    pool = _default_lxd_storage_pool()
    for machine_id, instance in machine_to_instance.items():
        existing_devices = _lxd_instance_device_names(instance)
        for disk_index in range(disk_count):
            volume = _lxd_osd_volume_name(machine_id, disk_index)
            device = f"osd-{disk_index}"
            if not _lxd_storage_volume_exists(pool, volume):
                run(
                    [
                        "lxc",
                        "storage",
                        "volume",
                        "create",
                        pool,
                        volume,
                        "--type=block",
                        f"size={disk_size}GiB",
                    ]
                )
            if device not in existing_devices:
                run(
                    [
                        "lxc",
                        "config",
                        "device",
                        "add",
                        instance,
                        device,
                        "disk",
                        f"pool={pool}",
                        f"source={volume}",
                    ]
                )


def _detach_lxd_osd_devices(machine_to_instance: dict[str, str]) -> None:
    for instance in machine_to_instance.values():
        for device in sorted(_lxd_instance_device_names(instance)):
            if device.startswith("osd-"):
                run(["lxc", "config", "device", "remove", instance, device])


def _create_lxd_nodes_impl(
    ctx_obj: dict[str, object],
    vm_data_disk_size: int,
    vm_data_disk_count: int,
    vm_count: int,
) -> None:
    controller = _controller_name(str(ctx_obj["substrate"]))
    model = CEPHTOOLS_MODEL
    existing = _juju_machine_instance_ids(controller, model)
    missing = max(0, vm_count - len(existing))
    if missing:
        run(
            [
                "juju",
                "add-machine",
                "-n",
                str(missing),
                "--constraints",
                "virt-type=virtual-machine",
                "-m",
                _juju_model_ref(controller, model),
            ]
        )
    machine_to_instance = _wait_for_lxd_juju_machines(controller, model, vm_count)
    _attach_lxd_osd_volumes(
        machine_to_instance,
        disk_count=vm_data_disk_count,
        disk_size=vm_data_disk_size,
    )


def _destroy_lxd_nodes_impl(ctx_obj: dict[str, object]) -> None:
    controller = _controller_name(str(ctx_obj["substrate"]))
    model = CEPHTOOLS_MODEL
    machine_to_instance = _juju_machine_instance_ids(controller, model)
    if not machine_to_instance:
        return
    _detach_lxd_osd_devices(machine_to_instance)
    cleanup_result = _cleanup_lxd_osd_volumes()
    if cleanup_result.failed:
        raise click.ClickException(cleanup_result.detail)
    run(
        [
            "juju",
            "remove-machine",
            *sorted(machine_to_instance, key=_machine_sort_key),
            "--force",
            "--no-wait",
            "--no-prompt",
            "-m",
            _juju_model_ref(controller, model),
        ]
    )


def _create_nodes_impl(
    ctx_obj: dict[str, object],
    vm_data_disk_size: int,
    vm_data_disk_count: int,
    vm_count: int,
) -> None:
    if vm_data_disk_size <= 0 or vm_data_disk_count <= 0:
        raise click.ClickException(
            "--vm-data-disk-size and --vm-data-disk-count must be positive."
        )
    if vm_count <= 0:
        raise click.ClickException("--vm-count must be a positive integer.")

    if ctx_obj.get("substrate") == SUBSTRATE_LXD:
        _create_lxd_nodes_impl(
            ctx_obj,
            vm_data_disk_size,
            vm_data_disk_count,
            vm_count,
        )
        return

    maas_vm_name = (
        ctx_obj.get("maas_vm_name")
        if ctx_obj.get("substrate") == SUBSTRATE_MAAS_VM
        else None
    )
    _get_lxd_vm_host_id(
        ctx_obj["admin"], ctx_obj["vmhost"], maas_vm_name=maas_vm_name
    )  # ensure host exists
    vm_host_name = ctx_obj["vmhost"]

    clouds = read_testenv_cloud_config()
    try:
        maas_cloud = clouds["maas-cloud"]
        maas_api_url = maas_cloud["endpoint"]
    except KeyError as exc:
        raise click.ClickException(
            "cloud.yaml is missing required maas-cloud endpoint."
        ) from exc

    credentials = read_testenv_credentials()
    try:
        maas_api_key = credentials["maas-cloud"]["admin"]["maas-oauth"]
    except KeyError as exc:
        raise click.ClickException(
            "cred.yaml is missing maas-cloud admin credentials."
        ) from exc

    network = read_testenv_network_config()
    try:
        primary_subnet_cidr = network["cidr"]
    except KeyError as exc:
        raise click.ClickException(
            "network.yaml is missing the primary subnet CIDR."
        ) from exc
    external_section = network.get("external")
    if not isinstance(external_section, dict):
        raise click.ClickException(
            "network.yaml is missing the external network configuration."
        )
    try:
        external_subnet_cidr = external_section["cidr"]
    except KeyError as exc:
        raise click.ClickException(
            "network.yaml is missing the external subnet CIDR."
        ) from exc

    terragrunt_dir = _resolve_terragrunt_dir()
    inputs_path = _write_ensure_nodes_inputs_file(
        terragrunt_dir,
        {
            "maas_api_url": maas_api_url,
            "maas_api_key": maas_api_key,
            "lxd_vm_host": vm_host_name,
            "vm_data_disk_size": vm_data_disk_size,
            "vm_data_disk_count": vm_data_disk_count,
            "vm_count": vm_count,
            "primary_subnet_cidr": primary_subnet_cidr,
            "external_subnet_cidr": external_subnet_cidr,
        },
    )
    click.echo(f"Saved Terragrunt inputs to {inputs_path}")

    terragrunt_args = [
        "terragrunt",
        "apply",
        "-auto-approve",
        "-parallelism=1",
    ]
    terragrunt_cmd = " ".join(terragrunt_args)
    run(
        f"cd {shlex.quote(str(terragrunt_dir))} && {terragrunt_cmd}",
        shell=True,
    )

    hostnames = _terragrunt_vm_hostnames(terragrunt_dir)
    _ensure_maas_tag(ctx_obj["admin"], CEPHTOOLS_TAG, maas_vm_name=maas_vm_name)
    hostname_to_system_id = _tag_maas_machines(
        ctx_obj["admin"], hostnames, CEPHTOOLS_TAG, maas_vm_name=maas_vm_name
    )
    _tag_data_disks(
        ctx_obj["admin"],
        hostnames,
        hostname_to_system_id,
        tag="osd",
        maas_vm_name=maas_vm_name,
    )


def _destroy_nodes_impl() -> None:
    terragrunt_dir = _resolve_terragrunt_dir()
    inputs_path = terragrunt_dir / ENSURE_NODES_INPUT_FILENAME
    if not inputs_path.exists():
        raise click.ClickException(
            f"Terragrunt input file {inputs_path} not found. Run 'cephtools testenv ensure-nodes' first."
        )

    click.echo(f"Destroying nodes using inputs from {inputs_path}")
    terragrunt_args = [
        "terragrunt",
        "destroy",
        "-auto-approve",
        "-parallelism=1",
    ]
    terragrunt_cmd = " ".join(terragrunt_args)
    run(
        f"cd {shlex.quote(str(terragrunt_dir))} && {terragrunt_cmd}",
        shell=True,
    )


def _terragrunt_dir_not_found_detail(detail: str) -> bool:
    return "Unable to locate terragrunt configuration directory." in detail


def _cleanup_destroy_nodes(*, dry_run: bool = False) -> CleanupPhaseResult:
    phase = "destroy nodes"
    if dry_run:
        return CleanupPhaseResult(
            phase,
            "ok",
            "dry-run: would destroy Terragrunt-managed nodes",
        )

    try:
        terragrunt_dir = _resolve_terragrunt_dir()
    except click.ClickException as exc:
        detail = str(exc)
        if _terragrunt_dir_not_found_detail(detail):
            return CleanupPhaseResult(
                phase,
                "skipped",
                "terragrunt configuration directory not found; no Terragrunt-managed nodes to destroy",
            )
        return CleanupPhaseResult(phase, "failed", detail)

    inputs_path = terragrunt_dir / ENSURE_NODES_INPUT_FILENAME
    if not inputs_path.exists():
        return CleanupPhaseResult(
            phase,
            "skipped",
            f"{inputs_path} not found",
        )

    try:
        _destroy_nodes_impl()
    except (click.ClickException, subprocess.CalledProcessError) as exc:
        return CleanupPhaseResult(phase, "failed", str(exc))

    return CleanupPhaseResult(phase, "ok", f"destroyed nodes using {inputs_path}")


def _cleanup_kill_controller(
    controller_name: str, *, dry_run: bool = False
) -> CleanupPhaseResult:
    phase = f"kill controller {controller_name}"
    if dry_run:
        return CleanupPhaseResult(
            phase,
            "ok",
            f"dry-run: would kill controller {controller_name}",
        )

    if shutil.which("juju") is None:
        return CleanupPhaseResult(phase, "skipped", "juju command not found")

    juju = jubilant.Juju()
    try:
        if not _juju_controller_exists(juju, controller_name):
            return CleanupPhaseResult(
                phase,
                "skipped",
                f"controller {controller_name} not found",
            )
    except jubilant.CLIError as exc:
        return CleanupPhaseResult(phase, "failed", _format_juju_error(exc))

    try:
        run(
            [
                "juju",
                "kill-controller",
                controller_name,
                "--no-prompt",
                "--timeout",
                "2m",
            ]
        )
    except subprocess.CalledProcessError as exc:
        return CleanupPhaseResult(phase, "failed", str(exc))

    return CleanupPhaseResult(phase, "ok", f"killed controller {controller_name}")


def _cleanup_delete_vm_host(
    admin: str,
    vmhost: str,
    *,
    maas_vm_name: str | None = None,
    dry_run: bool = False,
) -> CleanupPhaseResult:
    phase = f"delete vm host {vmhost}"
    if dry_run:
        return CleanupPhaseResult(
            phase,
            "ok",
            f"dry-run: would delete MAAS VM host {vmhost}",
        )

    if maas_vm_name is None and shutil.which("maas") is None:
        return CleanupPhaseResult(phase, "skipped", "maas command not found")
    if maas_vm_name is not None and shutil.which("lxc") is None:
        return CleanupPhaseResult(phase, "skipped", "lxc command not found")

    try:
        if maas_vm_name is None:
            host_id = _get_lxd_vm_host_id(admin, vmhost)
        else:
            host_id = _get_lxd_vm_host_id(admin, vmhost, maas_vm_name=maas_vm_name)
    except click.ClickException as exc:
        detail = str(exc)
        if _message_indicates_not_found(detail):
            return CleanupPhaseResult(phase, "skipped", f"VM host {vmhost} not found")
        return CleanupPhaseResult(phase, "failed", detail)
    except subprocess.CalledProcessError as exc:
        detail = _format_process_error(exc)
        if maas_vm_name is not None and "invalid choice" in detail and admin in detail:
            return CleanupPhaseResult(
                phase,
                "skipped",
                "MAAS CLI profile is not available in the MAAS VM",
            )
        return CleanupPhaseResult(phase, "failed", detail)

    try:
        _run_maas_cli(
            f"maas {shlex.quote(admin)} vm-host delete {host_id}",
            maas_vm_name=maas_vm_name,
        )
    except subprocess.CalledProcessError as exc:
        return CleanupPhaseResult(phase, "failed", _format_process_error(exc))

    return CleanupPhaseResult(phase, "ok", f"deleted MAAS VM host id {host_id}")


def _cleanup_delete_known_lxd_instances(*, dry_run: bool = False) -> CleanupPhaseResult:
    phase = "delete known LXD instances"
    instance_names = (WARMUP_VM_NAME,)
    if dry_run:
        instances = ", ".join(instance_names)
        return CleanupPhaseResult(
            phase,
            "ok",
            f"dry-run: would delete {instances}",
        )

    if shutil.which("lxc") is None:
        return CleanupPhaseResult(phase, "skipped", "lxc command not found")

    deleted: list[str] = []
    for instance_name in instance_names:
        info = run(["lxc", "info", instance_name], check=False, quiet=True)
        if info.returncode != 0:
            detail = _format_process_error(info)
            if _message_indicates_not_found(detail):
                continue
            return CleanupPhaseResult(
                phase,
                "failed",
                f"Failed to inspect LXD instance {instance_name}: {detail}",
            )
        try:
            run(["lxc", "delete", instance_name, "--force"])
        except subprocess.CalledProcessError as exc:
            return CleanupPhaseResult(phase, "failed", _format_process_error(exc))
        deleted.append(instance_name)

    if not deleted:
        return CleanupPhaseResult(
            phase,
            "skipped",
            "No known testenv-owned LXD instances found",
        )

    return CleanupPhaseResult(phase, "ok", f"deleted {', '.join(deleted)}")


def _cleanup_delete_maas_vm(
    vm_name: str, *, dry_run: bool = False
) -> CleanupPhaseResult:
    phase = f"delete MAAS VM {vm_name}"
    if dry_run:
        return CleanupPhaseResult(phase, "ok", f"dry-run: would delete {vm_name}")
    if shutil.which("lxc") is None:
        return CleanupPhaseResult(phase, "skipped", "lxc command not found")
    info = run(["lxc", "info", vm_name], check=False, quiet=True)
    if info.returncode != 0:
        detail = _format_process_error(info)
        if _message_indicates_not_found(detail):
            return CleanupPhaseResult(phase, "skipped", f"{vm_name} not found")
        return CleanupPhaseResult(phase, "failed", detail)
    try:
        run(["lxc", "delete", vm_name, "--force"])
    except subprocess.CalledProcessError as exc:
        return CleanupPhaseResult(phase, "failed", _format_process_error(exc))
    return CleanupPhaseResult(phase, "ok", f"deleted {vm_name}")


def _cleanup_delete_lxd_project(
    project: str, *, dry_run: bool = False
) -> CleanupPhaseResult:
    phase = f"delete LXD project {project}"
    if dry_run:
        return CleanupPhaseResult(phase, "ok", f"dry-run: would delete {project}")
    if shutil.which("lxc") is None:
        return CleanupPhaseResult(phase, "skipped", "lxc command not found")
    if not _lxd_project_exists(project):
        return CleanupPhaseResult(phase, "skipped", f"project {project} not found")
    instances = run(
        ["lxc", "list", "--project", project, "--format", "json"],
        check=False,
        quiet=True,
    )
    if instances.returncode == 0:
        try:
            payload = json.loads(instances.stdout or "[]")
        except json.JSONDecodeError:
            payload = []
        if payload:
            return CleanupPhaseResult(
                phase,
                "skipped",
                f"project {project} still contains instances",
            )
    try:
        run(["lxc", "project", "delete", project])
    except subprocess.CalledProcessError as exc:
        return CleanupPhaseResult(phase, "failed", _format_process_error(exc))
    return CleanupPhaseResult(phase, "ok", f"deleted project {project}")


def _cleanup_remove_state_files(*, dry_run: bool = False) -> CleanupPhaseResult:
    phase = "remove state files"
    if dry_run:
        filenames = ", ".join(TESTENV_STATE_FILENAMES)
        return CleanupPhaseResult(
            phase,
            "ok",
            f"dry-run: would remove {filenames}",
        )

    deleted: list[str] = []
    for filename in TESTENV_STATE_FILENAMES:
        path = get_state_file(filename, ensure_parent=False)
        if not path.exists():
            continue
        path.unlink(missing_ok=True)
        deleted.append(filename)

    if not deleted:
        return CleanupPhaseResult(phase, "skipped", "No generated state files found")

    return CleanupPhaseResult(phase, "ok", f"removed {', '.join(deleted)}")


def _cleanup_remove_terragrunt_inputs(*, dry_run: bool = False) -> CleanupPhaseResult:
    phase = "remove terragrunt inputs"
    if dry_run:
        return CleanupPhaseResult(
            phase,
            "ok",
            f"dry-run: would remove {ENSURE_NODES_INPUT_FILENAME}",
        )

    try:
        terragrunt_dir = _resolve_terragrunt_dir()
    except click.ClickException as exc:
        detail = str(exc)
        if _terragrunt_dir_not_found_detail(detail):
            return CleanupPhaseResult(
                phase,
                "skipped",
                "terragrunt configuration directory not found; no Terragrunt inputs to remove",
            )
        return CleanupPhaseResult(phase, "failed", detail)

    inputs_path = terragrunt_dir / ENSURE_NODES_INPUT_FILENAME
    if not inputs_path.exists():
        return CleanupPhaseResult(phase, "skipped", f"{inputs_path} not found")

    inputs_path.unlink(missing_ok=True)
    return CleanupPhaseResult(phase, "ok", f"removed {inputs_path}")


def _installed_apt_packages(
    *,
    prefixes: tuple[str, ...] = (),
    exact_names: tuple[str, ...] = (),
) -> list[str]:
    result = run(
        ["dpkg-query", "-W", "-f=${binary:Package}\t${Status}\n"],
        check=False,
        quiet=True,
    )
    if result.returncode != 0:
        raise click.ClickException(_format_process_error(result))

    matches: list[str] = []
    exact = set(exact_names)
    for line in result.stdout.splitlines():
        package, _, status = line.partition("\t")
        if status.strip() != "install ok installed":
            continue
        if package in exact or any(package.startswith(prefix) for prefix in prefixes):
            matches.append(package)
    return sorted(matches)


def _cleanup_remove_snap(name: str, *, dry_run: bool = False) -> CleanupPhaseResult:
    phase = f"remove snap {name}"
    if dry_run:
        return CleanupPhaseResult(phase, "ok", f"dry-run: would remove snap {name}")

    result = run(["snap", "list", name], check=False, quiet=True)
    if result.returncode != 0:
        detail = _format_process_error(result)
        if _message_indicates_not_found(detail):
            return CleanupPhaseResult(phase, "skipped", f"snap {name} is not installed")
        return CleanupPhaseResult(phase, "failed", detail)

    try:
        run(["sudo", "snap", "remove", "--purge", name])
    except subprocess.CalledProcessError as exc:
        return CleanupPhaseResult(phase, "failed", _format_process_error(exc))

    return CleanupPhaseResult(phase, "ok", f"removed snap {name}")


def _cleanup_remove_user_paths(
    phase: str,
    paths: tuple[Path, ...],
    *,
    dry_run: bool = False,
) -> CleanupPhaseResult:
    existing = [path for path in paths if path.exists() or path.is_symlink()]
    if not existing:
        return CleanupPhaseResult(phase, "skipped", "No matching paths found")

    if dry_run:
        rendered = ", ".join(str(path) for path in existing)
        return CleanupPhaseResult(phase, "ok", f"dry-run: would remove {rendered}")

    removed: list[str] = []
    try:
        for path in existing:
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
            else:
                path.unlink(missing_ok=True)
            removed.append(str(path))
    except OSError as exc:
        return CleanupPhaseResult(phase, "failed", str(exc))

    return CleanupPhaseResult(phase, "ok", f"removed {', '.join(removed)}")


def _root_path_exists(path: str) -> bool:
    result = run(["sudo", "test", "-e", path], check=False, quiet=True)
    if result.returncode == 0:
        return True
    if result.returncode == 1:
        return False
    raise click.ClickException(_format_process_error(result))


def _cleanup_remove_root_paths(
    phase: str,
    paths: tuple[str, ...],
    *,
    dry_run: bool = False,
) -> CleanupPhaseResult:
    try:
        existing = [path for path in paths if _root_path_exists(path)]
    except click.ClickException as exc:
        return CleanupPhaseResult(phase, "failed", str(exc))

    if not existing:
        return CleanupPhaseResult(phase, "skipped", "No matching paths found")

    if dry_run:
        rendered = ", ".join(existing)
        return CleanupPhaseResult(phase, "ok", f"dry-run: would remove {rendered}")

    try:
        run(["sudo", "rm", "-rf", *existing])
    except subprocess.CalledProcessError as exc:
        return CleanupPhaseResult(phase, "failed", _format_process_error(exc))

    return CleanupPhaseResult(phase, "ok", f"removed {', '.join(existing)}")


def _cleanup_purge_apt_packages(
    phase: str,
    *,
    prefixes: tuple[str, ...] = (),
    exact_names: tuple[str, ...] = (),
    dry_run: bool = False,
) -> CleanupPhaseResult:
    try:
        packages = _installed_apt_packages(prefixes=prefixes, exact_names=exact_names)
    except click.ClickException as exc:
        return CleanupPhaseResult(phase, "failed", str(exc))

    if not packages:
        return CleanupPhaseResult(
            phase, "skipped", "No matching apt packages installed"
        )

    if dry_run:
        rendered = ", ".join(packages)
        return CleanupPhaseResult(phase, "ok", f"dry-run: would purge {rendered}")

    try:
        run(
            [
                "sudo",
                "env",
                "DEBIAN_FRONTEND=noninteractive",
                "apt-get",
                "-y",
                "purge",
                *packages,
            ]
        )
    except subprocess.CalledProcessError as exc:
        return CleanupPhaseResult(phase, "failed", _format_process_error(exc))

    return CleanupPhaseResult(phase, "ok", f"purged {', '.join(packages)}")


def _maas_ppa_source_paths() -> list[Path]:
    sources_dir = Path("/etc/apt/sources.list.d")
    if not sources_dir.exists():
        return []

    markers = ("ppa.launchpadcontent.net/maas/", "ppa.launchpad.net/maas/")
    matches: list[Path] = []
    for path in sorted(sources_dir.iterdir()):
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if any(marker in text for marker in markers):
            matches.append(path)
    return matches


def _cleanup_remove_maas_ppa_sources(*, dry_run: bool = False) -> CleanupPhaseResult:
    phase = "remove MAAS apt sources"
    paths = _maas_ppa_source_paths()
    if not paths:
        return CleanupPhaseResult(phase, "skipped", "No MAAS apt source files found")

    rendered = ", ".join(str(path) for path in paths)
    if dry_run:
        return CleanupPhaseResult(phase, "ok", f"dry-run: would remove {rendered}")

    try:
        run(["sudo", "rm", "-f", *[str(path) for path in paths]])
    except subprocess.CalledProcessError as exc:
        return CleanupPhaseResult(phase, "failed", _format_process_error(exc))

    return CleanupPhaseResult(phase, "ok", f"removed {rendered}")


def _cleanup_apt_autoremove(*, dry_run: bool = False) -> CleanupPhaseResult:
    phase = "apt autoremove --purge"
    if dry_run:
        return CleanupPhaseResult(
            phase,
            "ok",
            "dry-run: would run apt-get -y autoremove --purge",
        )

    try:
        run(
            [
                "sudo",
                "env",
                "DEBIAN_FRONTEND=noninteractive",
                "apt-get",
                "-y",
                "autoremove",
                "--purge",
            ]
        )
    except subprocess.CalledProcessError as exc:
        return CleanupPhaseResult(phase, "failed", _format_process_error(exc))

    return CleanupPhaseResult(phase, "ok", "completed apt autoremove --purge")


def _cleanup_apt_update(*, dry_run: bool = False) -> CleanupPhaseResult:
    phase = "apt update"
    if dry_run:
        return CleanupPhaseResult(phase, "ok", "dry-run: would run apt-get update")

    try:
        run(["sudo", "apt-get", "update"])
    except subprocess.CalledProcessError as exc:
        return CleanupPhaseResult(phase, "failed", _format_process_error(exc))

    return CleanupPhaseResult(phase, "ok", "updated apt package lists")


def _cleanup_restore_systemd_timesyncd(*, dry_run: bool = False) -> CleanupPhaseResult:
    phase = "restore systemd-timesyncd"
    if dry_run:
        return CleanupPhaseResult(
            phase,
            "ok",
            "dry-run: would install and enable systemd-timesyncd if needed",
        )

    try:
        installed = bool(_installed_apt_packages(exact_names=("systemd-timesyncd",)))
    except click.ClickException as exc:
        return CleanupPhaseResult(phase, "failed", str(exc))

    try:
        if not installed:
            run(
                [
                    "sudo",
                    "env",
                    "DEBIAN_FRONTEND=noninteractive",
                    "apt-get",
                    "-y",
                    "install",
                    "systemd-timesyncd",
                ]
            )
        run(["sudo", "systemctl", "enable", "--now", "systemd-timesyncd"])
    except subprocess.CalledProcessError as exc:
        return CleanupPhaseResult(phase, "failed", _format_process_error(exc))

    if installed:
        return CleanupPhaseResult(phase, "ok", "enabled systemd-timesyncd")
    return CleanupPhaseResult(phase, "ok", "installed and enabled systemd-timesyncd")


def _cleanup_lxd_osd_volumes(*, dry_run: bool = False) -> CleanupPhaseResult:
    phase = "delete LXD OSD volumes"
    volume_pattern = f"{CEPHTOOLS_MODEL}-<machine-id>-osd-<disk-index>"
    if dry_run:
        return CleanupPhaseResult(
            phase,
            "ok",
            f"dry-run: would delete LXD custom block volumes matching {volume_pattern}",
        )
    if shutil.which("lxc") is None:
        return CleanupPhaseResult(phase, "skipped", "lxc command not found")
    try:
        pool = _default_lxd_storage_pool()
        result = run(["lxc", "storage", "volume", "list", pool, "--format", "json"])
        volumes = json.loads(result.stdout or "[]")
    except (
        click.ClickException,
        subprocess.CalledProcessError,
        json.JSONDecodeError,
    ) as exc:
        return CleanupPhaseResult(phase, "failed", str(exc))

    deleted: list[str] = []
    for volume in volumes:
        if not isinstance(volume, dict):
            continue
        name = volume.get("name")
        content_type = volume.get("content_type")
        volume_type = volume.get("type")
        if not isinstance(name, str) or not _is_lxd_osd_volume_name(name):
            continue
        if volume_type != "custom" or content_type != "block":
            continue
        try:
            run(["lxc", "storage", "volume", "delete", pool, f"custom/{name}"])
        except subprocess.CalledProcessError as exc:
            return CleanupPhaseResult(phase, "failed", _format_process_error(exc))
        deleted.append(name)

    if not deleted:
        return CleanupPhaseResult(phase, "skipped", "no matching LXD OSD volumes")
    return CleanupPhaseResult(phase, "ok", "deleted " + ", ".join(sorted(deleted)))


def _emit_cleanup_summary(results: list[CleanupPhaseResult]) -> None:
    click.echo("Cleanup summary:")
    for result in results:
        detail = f" ({result.detail})" if result.detail else ""
        click.echo(f"- {result.phase}: {result.outcome}{detail}")

    failures = sum(1 for result in results if result.failed)
    if failures:
        click.echo(f"Cleanup completed with {failures} failed phase(s).")
    else:
        click.echo("Cleanup completed without failures.")


# ---- click CLI ------------------------------------------------------------


@click.group(help="MAAS/LXD/Juju bootstrap CLI.")
@click.option(
    "--maas-version",
    default=DEFAULT_MAAS_VERSION,
    show_default=True,
    help="MAAS PPA version, e.g. 3.7",
)
@click.option(
    "--substrate",
    type=click.Choice(list(SUBSTRATES)),
    default=DEFAULT_SUBSTRATE,
    show_default=True,
    help="Choose the test environment substrate.",
)
@click.option(
    "--maas-vm-cpus",
    type=int,
    default=DEFAULT_MAAS_VM_CPUS,
    show_default=True,
    help="CPU limit for the MAAS VM.",
)
@click.option(
    "--maas-vm-memory",
    default=DEFAULT_MAAS_VM_MEMORY,
    show_default=True,
    help="Memory limit for the MAAS VM.",
)
@click.option(
    "--maas-vm-disk",
    default=DEFAULT_MAAS_VM_DISK,
    show_default=True,
    help="Root disk size for the MAAS VM.",
)
@click.option(
    "--maas-vm-image",
    default=DEFAULT_MAAS_VM_IMAGE,
    show_default=True,
    help="Image used for the MAAS VM.",
)
@click.pass_context
def cli(
    ctx,
    maas_version,
    substrate,
    maas_vm_cpus,
    maas_vm_memory,
    maas_vm_disk,
    maas_vm_image,
):
    ctx.ensure_object(dict)
    ctx.obj.update(
        admin=MAAS_ADMIN,
        admin_pw=MAAS_ADMIN_PASSWORD,
        admin_mail=MAAS_ADMIN_EMAIL,
        maas_version=maas_version,
        lxdbridge=LXD_BRIDGE,
        substrate=substrate,
        maas_lxdbridge=MAAS_LXD_BRIDGE,
        maas_vm_name=MAAS_VM_NAME,
        maas_vm_cpus=maas_vm_cpus,
        maas_vm_memory=maas_vm_memory,
        maas_vm_disk=maas_vm_disk,
        maas_vm_ip=None,
        maas_vm_image=maas_vm_image,
        maas_lxd_project=MAAS_LXD_PROJECT,
        vmhost=MAAS_VM_HOST,
        ip=primary_ip(),
    )
    ctx.obj["maas_url"] = f"http://{ctx.obj['ip']}:5240/MAAS"


def _ctx_maas_vm_name(ctx_obj: dict[str, object]) -> str | None:
    if ctx_obj.get("substrate") == SUBSTRATE_MAAS_VM:
        return str(ctx_obj["maas_vm_name"])
    return None


def _ctx_maas_vm_ip(ctx_obj: dict[str, object]) -> str:
    configured = ctx_obj.get("maas_vm_ip")
    return derive_maas_vm_ip(
        str(ctx_obj["maas_lxdbridge"]),
        str(configured) if configured else None,
    )


def _ctx_maas_url(ctx_obj: dict[str, object]) -> str:
    if ctx_obj.get("substrate") == SUBSTRATE_MAAS_VM:
        return f"http://{_ctx_maas_vm_ip(ctx_obj)}:5240/MAAS"
    return str(ctx_obj["maas_url"])


def _ctx_maas_bridge(ctx_obj: dict[str, object]) -> str:
    if ctx_obj.get("substrate") == SUBSTRATE_MAAS_VM:
        return str(ctx_obj["maas_lxdbridge"])
    return str(ctx_obj["lxdbridge"])


@cli.command(
    "install-deps",
    help="Install substrate dependencies.",
)
@click.pass_context
def install_deps(ctx):
    substrate = ctx.obj["substrate"]
    if substrate == SUBSTRATE_MAAS_HOST:
        install_maas_deb(ctx.obj["maas_version"])
    ensure_snap("lxd")
    ensure_snap("terraform", classic=True)
    if substrate == SUBSTRATE_LXD:
        ensure_snap("juju")
    else:
        ensure_terragrunt()
    lxd_ready()
    click.echo("deps installed.")


@cli.command("lxd-init", help="Initialize LXD and tweak bridge.")
@click.pass_context
def lxd_init_cmd(ctx):
    substrate = ctx.obj["substrate"]
    if substrate == SUBSTRATE_MAAS_VM:
        lxd_init_vm_impl(
            ctx.obj["lxdbridge"],
            ctx.obj["maas_lxdbridge"],
            ctx.obj["maas_lxd_project"],
        )
    elif substrate == SUBSTRATE_LXD:
        lxd_init_lxd_impl(ctx.obj["lxdbridge"])
    else:
        lxd_init_impl(ctx.obj["ip"], ctx.obj["lxdbridge"])
    verify_lxd(ctx.obj["lxdbridge"])
    if substrate == SUBSTRATE_MAAS_HOST:
        lxd_warmup()
    click.echo("lxd ready.")


@cli.command(
    "maas-vm-init",
    help="Create the isolated MAAS VM and install MAAS snap inside it.",
)
@click.pass_context
def maas_vm_init_cmd(ctx):
    if ctx.obj["substrate"] != SUBSTRATE_MAAS_VM:
        raise click.ClickException("maas-vm-init requires --substrate maas-vm")
    maas_vm_ip = _ctx_maas_vm_ip(ctx.obj)
    maas_url = _ctx_maas_url(ctx.obj)
    api_key = maas_vm_init_impl(
        ctx.obj["maas_vm_name"],
        ctx.obj["maas_vm_image"],
        ctx.obj["maas_vm_cpus"],
        ctx.obj["maas_vm_memory"],
        ctx.obj["maas_vm_disk"],
        ctx.obj["maas_lxdbridge"],
        maas_vm_ip,
        maas_url,
        ctx.obj["admin"],
        ctx.obj["admin_pw"],
        ctx.obj["admin_mail"],
        ctx.obj["maas_version"],
    )
    write_cloud_yaml(maas_vm_ip)
    write_cred_yaml(api_key)
    ctx.obj["maas_url"] = maas_url
    ctx.obj["maas_vm_ip"] = maas_vm_ip
    click.echo(f"MAAS VM initialized at {maas_url}; cloud.yaml and cred.yaml written.")


@cli.command(
    "maas-init",
    help="Configure PostgreSQL-backed MAAS, create admin, and login.",
)
@click.pass_context
def maas_init_cmd(ctx):
    if ctx.obj["substrate"] == SUBSTRATE_LXD:
        click.echo("maas-init skipped for --substrate lxd.")
        return
    if ctx.obj["substrate"] == SUBSTRATE_MAAS_VM:
        ctx.invoke(maas_vm_init_cmd)
        return
    dns_preflight()
    maas_init_impl(
        ctx.obj["maas_url"],
        ctx.obj["admin"],
        ctx.obj["admin_pw"],
        ctx.obj["admin_mail"],
    )
    api_key = maas_api_key(ctx.obj["admin"])
    maas_login(ctx.obj["maas_url"], ctx.obj["admin"], api_key)
    time.sleep(5)
    verify_maas(ctx.obj["admin"])
    configure_maas_bind9_ipv4()
    dns_preflight()
    click.echo("maas initialized, bind9 configured, and logged in.")
    # Write cloud.yaml now; cred.yaml later in juju-init after health checks again.
    write_cloud_yaml(ctx.obj["ip"])
    click.echo("cloud.yaml written.")


@cli.command(
    "configure-bind9",
    help="Configure MAAS bind9 IPv4 to listen on all detected IPv4 addresses.",
)
@click.pass_context
def configure_bind9(ctx):
    if ctx.obj["substrate"] == SUBSTRATE_LXD:
        click.echo("configure-bind9 skipped for --substrate lxd.")
        return
    configure_maas_bind9_ipv4()
    click.echo("maas bind9 configured.")


@cli.command("register-vm-host", help="Register local LXD as MAAS VM host.")
@click.pass_context
def register_vm_host(ctx):
    if ctx.obj["substrate"] == SUBSTRATE_LXD:
        click.echo("register-vm-host skipped for --substrate lxd.")
        return
    maas_vm_name = _ctx_maas_vm_name(ctx.obj)
    if ctx.obj["substrate"] == SUBSTRATE_MAAS_VM:
        _, lxd_api_ip = lxd_network_cidr_and_gateway(ctx.obj["maas_lxdbridge"])
        project = ctx.obj["maas_lxd_project"]
    else:
        lxd_api_ip = ctx.obj["ip"]
        project = "default"
    register_lxd_vmhost_impl(
        ctx.obj["admin"],
        ctx.obj["vmhost"],
        lxd_api_ip,
        project=project,
        maas_vm_name=maas_vm_name,
    )
    import_boot_resources(ctx.obj["admin"], maas_vm_name=maas_vm_name)
    _wait_for_vm_host_architecture(
        ctx.obj["admin"],
        ctx.obj["vmhost"],
        REQUIRED_BOOT_ARCHITECTURE,
        maas_vm_name=maas_vm_name,
    )
    click.echo(
        "vm host registered, boot resources import complete, and required architecture available."
    )


@cli.command(
    "configure-network",
    help="Configure gateway, dynamic pool, and enable DHCP on VLAN.",
)
@click.pass_context
def configure_network(ctx):
    if ctx.obj["substrate"] == SUBSTRATE_LXD:
        configure_lxd_juju_network(
            controller=_controller_name(SUBSTRATE_LXD),
            model=CEPHTOOLS_MODEL,
            lxdbridge=ctx.obj["lxdbridge"],
        )
        click.echo(
            f"space '{EXTERNAL_SPACE_NAME}' configured from LXD network {EXT_LXD_NETWORK}."
        )
        return

    maas_vm_name = _ctx_maas_vm_name(ctx.obj)
    bridge = _ctx_maas_bridge(ctx.obj)
    if ctx.obj["substrate"] == SUBSTRATE_MAAS_VM:
        cidr, gw = lxd_network_cidr_and_gateway(bridge)
        reserved_ips = {gw, _ctx_maas_vm_ip(ctx.obj)}
    else:
        cidr, gw = route_info(bridge)
        reserved_ips = {gw}
    sid, fabric_id, vlan_id, rack_sysid = maas_subnet_ids(
        ctx.obj["admin"], cidr, maas_vm_name=maas_vm_name
    )
    update_subnet_gateway(ctx.obj["admin"], sid, gw, maas_vm_name=maas_vm_name)
    start_ip, end_ip = create_dynamic_iprange(
        ctx.obj["admin"],
        sid,
        cidr,
        reserved_ips=reserved_ips,
        maas_vm_name=maas_vm_name,
    )
    enable_vlan_dhcp(
        ctx.obj["admin"], fabric_id, vlan_id, rack_sysid, maas_vm_name=maas_vm_name
    )
    click.echo(f"network configured on {bridge} ({cidr}, gw {gw}).")
    space_id = create_space(
        ctx.obj["admin"], JUJU_SPACE_NAME, maas_vm_name=maas_vm_name
    )
    assign_space_to_vlan(
        ctx.obj["admin"], fabric_id, vlan_id, space_id, maas_vm_name=maas_vm_name
    )
    click.echo(f"space '{JUJU_SPACE_NAME}' ({space_id}) created and assigned to VLAN.")

    if ctx.obj["substrate"] == SUBSTRATE_MAAS_HOST:
        ext_cidr, ext_gw = lxd_network_cidr_and_gateway(EXT_LXD_NETWORK)
        ext_sid, ext_fabric_id, ext_vlan_id, ext_rack_sysid = maas_subnet_ids(
            ctx.obj["admin"], ext_cidr
        )
        update_subnet_gateway(ctx.obj["admin"], ext_sid, ext_gw)
        ext_start_ip, ext_end_ip = create_dynamic_iprange(
            ctx.obj["admin"], ext_sid, ext_cidr
        )
        enable_vlan_dhcp(ctx.obj["admin"], ext_fabric_id, ext_vlan_id, ext_rack_sysid)
        click.echo(
            f"network configured on {EXT_LXD_NETWORK} ({ext_cidr}, gw {ext_gw})."
        )
        ext_space_id = create_space(ctx.obj["admin"], EXTERNAL_SPACE_NAME)
        assign_space_to_vlan(ctx.obj["admin"], ext_fabric_id, ext_vlan_id, ext_space_id)
        click.echo(
            f"space '{EXTERNAL_SPACE_NAME}' ({ext_space_id}) created and assigned to VLAN."
        )
    else:
        ext_cidr, ext_gw = cidr, gw
        ext_start_ip, ext_end_ip = start_ip, end_ip
        ext_sid, ext_fabric_id, ext_vlan_id = sid, fabric_id, vlan_id
        ext_rack_sysid, ext_space_id = rack_sysid, space_id
    network_yaml = "\n".join(
        [
            "network:",
            f"  bridge: {bridge}",
            f"  cidr: {cidr}",
            f"  gateway: {gw}",
            "  dynamic_range:",
            f"    start: {start_ip}",
            f"    end: {end_ip}",
            f"  subnet_id: {sid}",
            f"  fabric_id: {fabric_id}",
            f"  vlan_id: {vlan_id}",
            f"  rack_sysid: {rack_sysid}",
            f"  space_id: {space_id}",
            "  external:",
            f"    bridge: {EXT_LXD_NETWORK if ctx.obj['substrate'] == SUBSTRATE_MAAS_HOST else bridge}",
            f"    cidr: {ext_cidr}",
            f"    gateway: {ext_gw}",
            "    dynamic_range:",
            f"      start: {ext_start_ip}",
            f"      end: {ext_end_ip}",
            f"    subnet_id: {ext_sid}",
            f"    fabric_id: {ext_fabric_id}",
            f"    vlan_id: {ext_vlan_id}",
            f"    rack_sysid: {ext_rack_sysid}",
            f"    space_id: {ext_space_id}",
            "",
        ]
    )
    network_path = get_state_file("network.yaml")
    network_path.write_text(network_yaml)
    click.echo("network.yaml written with current network configuration.")


@cli.command(
    "ensure-nodes",
    help="Ensure the desired set of MAAS VMs exist using terragrunt and testenv configuration files.",
)
@click.option(
    "--vm-data-disk-size",
    type=int,
    default=8,
    show_default=True,
    help="Size in GB for each data disk attached to the VMs.",
)
@click.option(
    "--vm-data-disk-count",
    type=int,
    default=1,
    show_default=True,
    help="Number of data disks to attach to each VM.",
)
@click.option(
    "--vm-count",
    type=int,
    default=6,
    show_default=True,
    help="Number of LXD VMs to create.",
)
@click.pass_context
def ensure_nodes(
    ctx,
    vm_data_disk_size: int,
    vm_data_disk_count: int,
    vm_count: int,
) -> None:
    _create_nodes_impl(ctx.obj, vm_data_disk_size, vm_data_disk_count, vm_count)
    if ctx.obj["substrate"] == SUBSTRATE_LXD:
        click.echo("Juju LXD machines ensured and OSD block volumes attached.")
    else:
        click.echo("Terragrunt apply completed; MAAS will reconcile VM nodes.")


@cli.command(
    "destroy-nodes",
    help="Destroy MAAS VMs previously created by ensure-nodes using saved Terragrunt inputs.",
)
@click.pass_context
def destroy_nodes(ctx):
    if ctx.obj["substrate"] == SUBSTRATE_LXD:
        _destroy_lxd_nodes_impl(ctx.obj)
        click.echo("Juju LXD machines and OSD block volumes removed.")
        return
    _destroy_nodes_impl()
    click.echo("Terragrunt destroy completed; MAAS will reconcile VM removals.")


@cli.command(
    "cleanup",
    help="Reclaim testenv-managed resources and generated state without uninstalling the host toolchain.",
)
@click.option(
    "--dry-run", is_flag=True, help="Print cleanup actions without executing them."
)
@click.option(
    "--keep-nodes",
    is_flag=True,
    help="Do not destroy managed nodes; also preserve the Juju controller.",
)
@click.option(
    "--keep-controller",
    is_flag=True,
    help="Do not destroy the Juju controller.",
)
@click.option(
    "--keep-vm-host",
    is_flag=True,
    help="Do not delete the MAAS VM host registration.",
)
@click.option(
    "--keep-lxd-instances",
    is_flag=True,
    help="Do not delete known testenv-owned LXD instances.",
)
@click.option(
    "--keep-maas-vm",
    is_flag=True,
    help="Do not delete the isolated MAAS VM in --substrate maas-vm.",
)
@click.option(
    "--keep-state",
    is_flag=True,
    help="Do not remove generated state files or Terragrunt inputs.",
)
@click.option(
    "--purge-installed",
    is_flag=True,
    help=(
        "Also remove the testenv-installed toolchain for maximum isolation "
        "(MAAS, PostgreSQL, LXD, Juju, Terraform, Terragrunt, and local Juju state). "
        "Incompatible with --keep-* flags."
    ),
)
@click.pass_context
def cleanup(
    ctx,
    dry_run: bool,
    keep_nodes: bool,
    keep_controller: bool,
    keep_vm_host: bool,
    keep_lxd_instances: bool,
    keep_maas_vm: bool,
    keep_state: bool,
    purge_installed: bool,
) -> None:
    preserve_flags = {
        "--keep-nodes": keep_nodes,
        "--keep-controller": keep_controller,
        "--keep-vm-host": keep_vm_host,
        "--keep-lxd-instances": keep_lxd_instances,
        "--keep-maas-vm": keep_maas_vm,
        "--keep-state": keep_state,
    }
    incompatible_flags = [flag for flag, enabled in preserve_flags.items() if enabled]
    if purge_installed and incompatible_flags:
        joined_flags = ", ".join(incompatible_flags)
        raise click.ClickException(
            "--purge-installed cannot be combined with preservation flags: "
            f"{joined_flags}"
        )

    if dry_run:
        click.echo("Running cleanup in dry-run mode; no changes will be made.")

    substrate = ctx.obj["substrate"]
    maas_vm_name = _ctx_maas_vm_name(ctx.obj)
    controller_name = _controller_name(substrate)
    preserve_controller = keep_controller or keep_nodes
    controller_preservation_detail = (
        "preserved by --keep-nodes"
        if keep_nodes and not keep_controller
        else "preserved by --keep-controller"
    )

    results: list[CleanupPhaseResult] = []
    nodes_result: CleanupPhaseResult | None = None
    if substrate != SUBSTRATE_LXD:
        if keep_nodes:
            nodes_result = CleanupPhaseResult(
                "destroy nodes", "skipped", "preserved by --keep-nodes"
            )
        else:
            nodes_result = (
                _cleanup_destroy_nodes(dry_run=True)
                if dry_run
                else _cleanup_destroy_nodes()
            )
        results.append(nodes_result)

    if preserve_controller:
        results.append(
            CleanupPhaseResult(
                f"kill controller {controller_name}",
                "skipped",
                controller_preservation_detail,
            )
        )
    else:
        results.append(
            _cleanup_kill_controller(controller_name, dry_run=True)
            if dry_run
            else _cleanup_kill_controller(controller_name)
        )

    if substrate == SUBSTRATE_LXD:
        if keep_nodes:
            nodes_result = CleanupPhaseResult(
                "delete LXD OSD volumes", "skipped", "preserved by --keep-nodes"
            )
        elif keep_controller:
            nodes_result = CleanupPhaseResult(
                "delete LXD OSD volumes",
                "skipped",
                "preserved while controller is kept",
            )
        else:
            nodes_result = (
                _cleanup_lxd_osd_volumes(dry_run=True)
                if dry_run
                else _cleanup_lxd_osd_volumes()
            )
        results.append(nodes_result)
        results.append(
            CleanupPhaseResult(
                f"delete vm host {ctx.obj['vmhost']}",
                "skipped",
                "not applicable for --substrate lxd",
            )
        )
    elif keep_vm_host:
        results.append(
            CleanupPhaseResult(
                f"delete vm host {ctx.obj['vmhost']}",
                "skipped",
                "preserved by --keep-vm-host",
            )
        )
    else:
        if maas_vm_name is None:
            results.append(
                _cleanup_delete_vm_host(
                    ctx.obj["admin"], ctx.obj["vmhost"], dry_run=True
                )
                if dry_run
                else _cleanup_delete_vm_host(ctx.obj["admin"], ctx.obj["vmhost"])
            )
        else:
            results.append(
                _cleanup_delete_vm_host(
                    ctx.obj["admin"],
                    ctx.obj["vmhost"],
                    maas_vm_name=maas_vm_name,
                    dry_run=True,
                )
                if dry_run
                else _cleanup_delete_vm_host(
                    ctx.obj["admin"],
                    ctx.obj["vmhost"],
                    maas_vm_name=maas_vm_name,
                )
            )

    if keep_lxd_instances:
        results.append(
            CleanupPhaseResult(
                "delete known LXD instances",
                "skipped",
                "preserved by --keep-lxd-instances",
            )
        )
    else:
        results.append(
            _cleanup_delete_known_lxd_instances(dry_run=True)
            if dry_run
            else _cleanup_delete_known_lxd_instances()
        )

    if maas_vm_name is not None:
        if keep_maas_vm:
            results.append(
                CleanupPhaseResult(
                    f"delete MAAS VM {maas_vm_name}",
                    "skipped",
                    "preserved by --keep-maas-vm",
                )
            )
        else:
            results.append(
                _cleanup_delete_maas_vm(maas_vm_name, dry_run=True)
                if dry_run
                else _cleanup_delete_maas_vm(maas_vm_name)
            )
        results.append(
            _cleanup_delete_lxd_project(ctx.obj["maas_lxd_project"], dry_run=True)
            if dry_run
            else _cleanup_delete_lxd_project(ctx.obj["maas_lxd_project"])
        )

    if keep_state:
        results.extend(
            [
                CleanupPhaseResult(
                    "remove state files",
                    "skipped",
                    "preserved by --keep-state",
                ),
                CleanupPhaseResult(
                    "remove terragrunt inputs",
                    "skipped",
                    "preserved by --keep-state",
                ),
            ]
        )
    else:
        results.append(
            _cleanup_remove_state_files(dry_run=True)
            if dry_run
            else _cleanup_remove_state_files()
        )
        if substrate == SUBSTRATE_LXD:
            results.append(
                CleanupPhaseResult(
                    "remove terragrunt inputs",
                    "skipped",
                    "not applicable for --substrate lxd",
                )
            )
        elif keep_nodes:
            results.append(
                CleanupPhaseResult(
                    "remove terragrunt inputs",
                    "skipped",
                    "preserved while nodes are kept",
                )
            )
        elif nodes_result.failed and not purge_installed:
            results.append(
                CleanupPhaseResult(
                    "remove terragrunt inputs",
                    "skipped",
                    "preserved because node cleanup did not complete successfully",
                )
            )
        else:
            results.append(
                _cleanup_remove_terragrunt_inputs(dry_run=True)
                if dry_run
                else _cleanup_remove_terragrunt_inputs()
            )

    if purge_installed:
        base_purge_results = [
            _cleanup_remove_snap("juju", dry_run=True)
            if dry_run
            else _cleanup_remove_snap("juju"),
            _cleanup_remove_user_paths(
                "remove Juju local state",
                USER_JUJU_STATE_PATHS,
                dry_run=True,
            )
            if dry_run
            else _cleanup_remove_user_paths(
                "remove Juju local state",
                USER_JUJU_STATE_PATHS,
            ),
        ]
        results.extend(base_purge_results)
        if substrate == SUBSTRATE_LXD:
            results.append(
                _cleanup_remove_snap("lxd", dry_run=True)
                if dry_run
                else _cleanup_remove_snap("lxd")
            )
        else:
            results.extend(
                [
                    _cleanup_purge_apt_packages(
                        "purge MAAS apt packages",
                        prefixes=("maas", "python3-maas", "bind9"),
                        dry_run=True,
                    )
                    if dry_run
                    else _cleanup_purge_apt_packages(
                        "purge MAAS apt packages",
                        prefixes=("maas", "python3-maas", "bind9"),
                    ),
                    _cleanup_purge_apt_packages(
                        "purge PostgreSQL apt packages",
                        prefixes=("postgresql",),
                        dry_run=True,
                    )
                    if dry_run
                    else _cleanup_purge_apt_packages(
                        "purge PostgreSQL apt packages",
                        prefixes=("postgresql",),
                    ),
                    _cleanup_purge_apt_packages(
                        "purge testenv helper apt packages",
                        exact_names=("software-properties-common", "lxd-installer"),
                        dry_run=True,
                    )
                    if dry_run
                    else _cleanup_purge_apt_packages(
                        "purge testenv helper apt packages",
                        exact_names=("software-properties-common", "lxd-installer"),
                    ),
                    _cleanup_apt_autoremove(dry_run=True)
                    if dry_run
                    else _cleanup_apt_autoremove(),
                    _cleanup_remove_maas_ppa_sources(dry_run=True)
                    if dry_run
                    else _cleanup_remove_maas_ppa_sources(),
                    _cleanup_apt_update(dry_run=True)
                    if dry_run
                    else _cleanup_apt_update(),
                    _cleanup_restore_systemd_timesyncd(dry_run=True)
                    if dry_run
                    else _cleanup_restore_systemd_timesyncd(),
                    _cleanup_remove_snap("lxd", dry_run=True)
                    if dry_run
                    else _cleanup_remove_snap("lxd"),
                    _cleanup_remove_snap("terraform", dry_run=True)
                    if dry_run
                    else _cleanup_remove_snap("terraform"),
                    _cleanup_remove_root_paths(
                        "remove Terragrunt binary",
                        ("/usr/local/bin/terragrunt",),
                        dry_run=True,
                    )
                    if dry_run
                    else _cleanup_remove_root_paths(
                        "remove Terragrunt binary",
                        ("/usr/local/bin/terragrunt",),
                    ),
                    _cleanup_remove_root_paths(
                        "remove residual toolchain directories",
                        TESTENV_ROOT_RESIDUAL_PATHS,
                        dry_run=True,
                    )
                    if dry_run
                    else _cleanup_remove_root_paths(
                        "remove residual toolchain directories",
                        TESTENV_ROOT_RESIDUAL_PATHS,
                    ),
                ]
            )

    _emit_cleanup_summary(results)
    if any(result.failed for result in results):
        ctx.exit(1)


@cli.command(
    "juju-init",
    help="Install Juju and bootstrap the substrate controller.",
)
@click.pass_context
def juju_init(ctx):
    substrate = ctx.obj["substrate"]
    maas_vm_name = _ctx_maas_vm_name(ctx.obj)
    verify_lxd(ctx.obj["lxdbridge"])

    ensure_snap("juju")
    if _is_maas_substrate(substrate):
        # health checks before creds
        verify_maas(ctx.obj["admin"], maas_vm_name=maas_vm_name)
        _wait_for_vm_host_architecture(
            ctx.obj["admin"],
            ctx.obj["vmhost"],
            REQUIRED_BOOT_ARCHITECTURE,
            maas_vm_name=maas_vm_name,
        )
        api_key = maas_api_key(ctx.obj["admin"], maas_vm_name=maas_vm_name)
        write_cloud_yaml(_ctx_maas_vm_ip(ctx.obj) if maas_vm_name else ctx.obj["ip"])
        write_cred_yaml(api_key)

    bootstrapped = juju_onboard(substrate)
    if bootstrapped:
        click.echo("juju initialized and controller bootstrapped.")
    else:
        click.echo("juju already initialized.")


def _ensure_model_for_substrate(substrate: str) -> None:
    controller = _controller_name(substrate)
    constraint = _model_constraint(substrate)
    _ensure_juju_model(CEPHTOOLS_MODEL, controller=controller, constraint=constraint)
    if constraint:
        click.echo(
            f"Juju model '{CEPHTOOLS_MODEL}' ensured with constraint {constraint}."
        )
    else:
        click.echo(f"Juju model '{CEPHTOOLS_MODEL}' ensured.")


@cli.command(
    "install",
    help="Run all installation steps for the selected substrate.",
)
@click.pass_context
def install(ctx):
    """Run all testenv installation steps in sequence."""
    install_fault_handlers("install")
    emit("Starting full testenv installation...")
    substrate = ctx.obj["substrate"]

    if substrate == SUBSTRATE_LXD:
        steps = [
            (
                "install-deps",
                "Installing dependencies",
                lambda: ctx.invoke(install_deps),
            ),
            ("lxd-init", "Initializing LXD", lambda: ctx.invoke(lxd_init_cmd)),
            ("juju-init", "Initializing Juju", lambda: ctx.invoke(juju_init)),
            (
                "create-model",
                "Creating Juju model",
                lambda: _ensure_model_for_substrate(substrate),
            ),
            (
                "configure-network",
                "Configuring network",
                lambda: ctx.invoke(configure_network),
            ),
            (
                "warmup-juju-images",
                "Warming up Juju VM images",
                lambda: juju_warmup(),
            ),
        ]
    else:
        steps = [
            (
                "install-deps",
                "Installing dependencies",
                lambda: ctx.invoke(install_deps),
            ),
            ("lxd-init", "Initializing LXD", lambda: ctx.invoke(lxd_init_cmd)),
            ("maas-init", "Initializing MAAS", lambda: ctx.invoke(maas_init_cmd)),
            (
                "register-vm-host",
                "Registering VM host",
                lambda: ctx.invoke(register_vm_host),
            ),
            (
                "configure-network",
                "Configuring network",
                lambda: ctx.invoke(configure_network),
            ),
            ("juju-init", "Initializing Juju", lambda: ctx.invoke(juju_init)),
            (
                "create-model",
                "Creating Juju model",
                lambda: _ensure_model_for_substrate(substrate),
            ),
        ]

    total = len(steps)
    for index, (name, title, action) in enumerate(steps, start=1):
        step = f"{index}/{total}"
        click.echo(f"\n=== Step {step}: {title} ===")
        with operation(step, name):
            action()

    click.echo("\n=== Installation complete! ===")
    click.echo(f"Substrate: {substrate}")
    click.echo(f"Controller: {_controller_name(substrate)}")
    if _is_maas_substrate(substrate):
        click.echo(f"MAAS URL: {ctx.obj['maas_url']}")
        click.echo(f"Admin user: {ctx.obj['admin']}")
    click.echo("You can now use 'juju status' to check your controller.")
    mark_complete()


def main():
    try:
        cli(obj={})
    except subprocess.CalledProcessError as e:
        print(e.stdout)
        print(e.stderr, file=sys.stderr)
        sys.exit(e.returncode)


if __name__ == "__main__":
    main()
