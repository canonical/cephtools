#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
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
from cephtools.config import (
    load_cephtools_config,
    load_testenv_defaults,
)
from cephtools.state import get_state_file
from cephtools.terraform import ensure_terragrunt, terraform_root_candidates
from cephtools.testflinger import (
    read_testenv_cloud_config,
    read_testenv_credentials,
    read_testenv_network_config,
)

# ---- defaults from configuration -----------------------------------------
DEFAULTS = load_testenv_defaults()

CEPHTOOLS_TAG = DEFAULTS["maas_tag"]
CEPHTOOLS_MODEL = load_cephtools_config(ensure=True)["juju_model"]
MAAS_CONTROLLER = "maas-controller"
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


def _resolve_terragrunt_dir() -> Path:
    env_path = os.getenv("CEPHTOOLS_TERRAGRUNT_DIR")
    candidates: list[Path] = []
    if env_path:
        candidates.append(Path(env_path).expanduser())

    config = load_cephtools_config(ensure=True)
    if config:
        raw_config_path: object = config.get("terragrunt_dir")
        if raw_config_path is None:
            paths_section = config.get("paths")
            if isinstance(paths_section, dict):
                raw_config_path = paths_section.get("terragrunt_dir")
        if raw_config_path:
            if not isinstance(raw_config_path, str):
                raise click.ClickException(
                    "Configuration value 'terragrunt_dir' must be a string path."
                )
            candidates.append(Path(raw_config_path).expanduser())

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


def _ensure_maas_tag(admin: str, tag: str) -> None:
    result = run(f"maas {admin} tags read")
    try:
        tags = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise click.ClickException("Failed to parse MAAS tags output as JSON.") from exc

    for entry in tags:
        if isinstance(entry, dict) and entry.get("name") == tag:
            return

    run(f"maas {admin} tags create name={tag}")


def _tag_maas_machines(admin: str, hostnames: list[str], tag: str) -> dict[str, str]:
    if not hostnames:
        return {}

    result = run(f"maas {admin} machines read")
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
        run(f"maas {admin} tag update-nodes {tag} add={system_id}")

    if missing:
        click.echo(
            "Warning: Unable to tag machines not found in MAAS: "
            + ", ".join(sorted(missing)),
            err=True,
        )

    return hostname_to_system_id


def _tag_data_disks(
    admin: str, hostnames: list[str], hostname_to_system_id: dict[str, str], *, tag: str
) -> None:
    for hostname in hostnames:
        system_id = hostname_to_system_id.get(hostname)
        if not system_id:
            continue

        result = run(f"maas {admin} block-devices read {system_id}")
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

            run(f"maas {admin} block-device add-tag {system_id} {device_id} tag={tag}")


def _ensure_juju_model(model: str, *, constraint: str) -> None:
    juju = jubilant.Juju()
    try:
        models_output = juju.cli(
            "models",
            "--format",
            "json",
            "--controller",
            MAAS_CONTROLLER,
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
            juju.add_model(model, controller=MAAS_CONTROLLER)
        except jubilant.CLIError as exc:
            message = _format_juju_error(exc)
            raise click.ClickException(
                f"Failed to add Juju model '{model}': {message}"
            ) from exc

    juju_for_model = jubilant.Juju(model=f"{MAAS_CONTROLLER}:{model}")
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
        ("ipv4.dhcp", "false"),
        ("ipv6.dhcp", "false"),
    ):
        run(["lxc", "network", "set", name, f"{key}={value}"])


def ensure_lxd_network(name: str, *, ipv4_address: str | None = None) -> None:
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
            "dns.mode=none"
        )

    _set_lxd_network_no_dns_or_dhcp(name)


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

    while True:
        if _bind9_service_state() not in active_states and not _bind9_named_processes():
            return
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


def lxd_init_impl(ip, admin_pw, lxdbridge):
    _stop_bind9_for_lxd_setup()
    _wait_for_bind9_shutdown()
    try:
        run("sudo snap set lxd daemon.user.group=adm")
        # Use minimal init so LXD does not auto-create lxdbr0 with dnsmasq
        # enabled before we can disable DNS/DHCP. We create and configure the
        # managed bridges explicitly afterwards.
        _run_lxd_minimal_init()
        run(["lxc", "config", "set", "core.https_address", ":8443"])
        run(["lxc", "config", "set", "core.trust_password", admin_pw])
        ensure_lxd_network(lxdbridge)
        ensure_lxd_default_profile_network(lxdbridge)
        ensure_lxd_network(EXT_LXD_NETWORK)
        time.sleep(2)
    finally:
        _start_bind9_after_lxd_setup()


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
        while time.monotonic() < deadline:
            time.sleep(10)
            try:
                run(f"lxc exec {vm_name} -- apt-get update", check=True)
                success = True
                break
            except subprocess.CalledProcessError:
                click.echo("Warmup VM not ready yet, retrying...")

        if not success:
            click.echo("Warning: Warmup apt-get update timed out.")

    finally:
        run(f"lxc delete {vm_name} --force", check=False)


def _restart_system_resolver() -> None:
    run("sudo resolvectl flush-caches || true", shell=True, check=False)
    run("sudo systemctl restart systemd-resolved || true", shell=True, check=False)


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


def _maas_admin_exists(admin: str) -> bool:
    result = run(f"sudo maas apikey --username {admin}", check=False, quiet=True)
    return result.returncode == 0 and bool((result.stdout or "").strip())


def _ensure_maas_auth_ready() -> None:
    run(["sudo", "maas-region", "configauth", "--json"], quiet=True)


def maas_init_impl(maas_url, admin, admin_pw, admin_mail):
    already_initialized = _maas_is_initialized()
    if already_initialized:
        print(
            "MAAS already configured; ensuring database settings, services, and admin user."
        )

    _ensure_maas_postgres(admin_pw)
    _configure_maas_region(maas_url, admin_pw)
    _ensure_maas_auth_ready()

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


def maas_api_key(admin) -> str:
    out = run(f"sudo maas apikey --username {admin}")
    return out.stdout.strip()


def maas_login(maas_url, admin, api_key):
    run(f'maas login "{admin}" "{maas_url}" "{api_key}"')


def verify_maas(admin):
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
    _ = run(f"maas {admin} boot-resources read").stdout


def register_lxd_vmhost_impl(admin, vmhost, ip, admin_pw):
    try:
        existing_id = _get_lxd_vm_host_id(admin, vmhost)
    except click.ClickException:
        existing_id = None
    if existing_id is not None:
        click.echo(
            f"VM host '{vmhost}' already registered in MAAS (id {existing_id}); skipping create."
        )
        return
    run(
        " ".join(
            [
                f'maas "{admin}" vm-hosts create type=lxd',
                f'name="{vmhost}"',
                "project=default",
                f'power_address="https://{ip}:8443"',
                f'password="{admin_pw}"',
                "--debug",
                "|| true",
            ]
        ),
        shell=True,
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


def import_boot_resources(admin):
    """Import images, wait for them to become available."""
    run(f'maas "{admin}" boot-resources import')
    time.sleep(15)
    # read boot and loop until we have the required architecture
    for _ in range(120):
        out = run(f"maas {admin} boot-resources read").stdout
        resources = json.loads(out)
        arches = extract_arches(resources)
        if REQUIRED_BOOT_ARCHITECTURE in arches:
            click.echo(
                f"Found {REQUIRED_BOOT_ARCHITECTURE}, waiting for it to stabilize..."
            )
            time.sleep(30)
            # Final check to ensure it didn't disappear (e.g. failed download)
            out = run(f"maas {admin} boot-resources read").stdout
            resources = json.loads(out)
            arches = extract_arches(resources)
            if REQUIRED_BOOT_ARCHITECTURE in arches:
                return
            raise Exception(
                f"Boot resource {REQUIRED_BOOT_ARCHITECTURE} disappeared after import!"
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
    net = json.loads(run(f"lxc query /1.0/networks/{network_name}").stdout)
    config = net.get("config") or {}
    ipv4_address = config.get("ipv4.address")
    if not ipv4_address or ipv4_address.lower() == "none":
        raise RuntimeError(f"LXD network {network_name} lacks an IPv4 address")
    iface = ip_interface(ipv4_address)
    cidr = iface.network.with_prefixlen
    return str(cidr), str(iface.ip)


def maas_subnet_ids(admin, cidr):
    subnets = json.loads(run(f"maas {admin} subnets read").stdout)
    sid = next((s["id"] for s in subnets if s.get("cidr") == cidr), None)
    if sid is None:
        raise RuntimeError(f"MAAS subnet for {cidr} not found")
    subnet = json.loads(run(f"maas {admin} subnet read {sid}").stdout)
    fabric_id = subnet["vlan"]["fabric_id"]
    vlan_id = subnet["vlan"]["vid"]
    racks = json.loads(run(f"maas {admin} rack-controllers read").stdout)
    rack_sysid = racks[0]["system_id"]
    return sid, fabric_id, vlan_id, rack_sysid


def update_subnet_gateway(admin, subnet_id, gw):
    run(f"maas {admin} subnet update {subnet_id} gateway_ip={gw}")


def create_dynamic_iprange(admin, subnet_id, cidr):
    hosts = list(ip_network(cidr).hosts())
    if len(hosts) < 80:
        raise RuntimeError("subnet too small for 80 hosts")
    start_ip, end_ip = str(hosts[-80]), str(hosts[-1])
    run(
        f"maas {admin} ipranges create type=dynamic subnet={subnet_id} "
        f'start_ip="{start_ip}" end_ip="{end_ip}" || true',
        shell=True,
    )
    time.sleep(12)  # wait for MAAS to process
    return start_ip, end_ip


def enable_vlan_dhcp(admin, fabric_id, vlan_id, rack_sysid):
    run(
        f"maas {admin} vlan update {fabric_id} {vlan_id} dhcp_on=true primary_rack={rack_sysid}"
    )


def create_space(admin, space_name):
    spaces = json.loads(run(f"maas {admin} spaces read").stdout)
    space_id = next((s["id"] for s in spaces if s.get("name") == space_name), None)
    if space_id is not None:
        return space_id

    run(f'maas {admin} spaces create name="{space_name}"')
    spaces = json.loads(run(f"maas {admin} spaces read").stdout)
    space_id = next((s["id"] for s in spaces if s.get("name") == space_name), None)
    if space_id is None:
        raise RuntimeError(f"MAAS space '{space_name}' not found after creation")
    return space_id


def assign_space_to_vlan(admin, fabric_id, vlan_id, space_id):
    run(f"maas {admin} vlan update {fabric_id} {vlan_id} space={space_id}")


def _get_lxd_vm_host_id(admin: str, vmhost: str) -> str:
    result = run(f"maas {admin} vm-hosts read")
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
    raise click.ClickException(f"VM host '{vmhost}' not found in MAAS vm-hosts output.")


def _get_vm_host_architectures(admin: str, vmhost: str) -> list[str]:
    result = run(f"maas {admin} vm-hosts read")
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
    timeout: int = 600,
    interval: int = 6,
) -> None:
    """Poll until the MAAS VM host reports the required architecture."""
    deadline = time.monotonic() + timeout
    last_seen: list[str] = []
    while time.monotonic() < deadline:
        architectures = _get_vm_host_architectures(admin, vmhost)
        if architecture in architectures:
            return
        last_seen = architectures
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
    for _ in range(20):
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
        time.sleep(6)

    raise click.ClickException("juju controller machines not ready after timeout")


def juju_onboard() -> bool:
    cloud_path = get_state_file("cloud.yaml")
    cred_path = get_state_file("cred.yaml")
    juju = jubilant.Juju()
    if not _juju_cloud_exists(juju, "maas-cloud"):
        click.echo("Registering Juju cloud 'maas-cloud'.")
        try:
            juju.cli(
                "add-cloud",
                "maas-cloud",
                str(cloud_path),
                "--client",
                include_model=False,
            )
        except jubilant.CLIError as exc:
            if not _is_already_exists_error(exc):
                raise
            click.echo("Juju reports cloud already exists; continuing.")
    else:
        click.echo("Juju cloud 'maas-cloud' already registered; skipping.")

    if not _juju_credential_exists(juju, "maas-cloud", "admin"):
        click.echo("Adding Juju credential 'admin' for cloud 'maas-cloud'.")
        try:
            juju.cli(
                "add-credential",
                "maas-cloud",
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

    time.sleep(2)

    bootstrapped = False
    if not _juju_controller_exists(juju, MAAS_CONTROLLER):
        click.echo(f"Bootstrapping Juju controller '{MAAS_CONTROLLER}'.")
        juju.bootstrap(
            "maas-cloud",
            MAAS_CONTROLLER,
            bootstrap_constraints={"spaces": JUJU_SPACE_NAME},
            config={"juju-mgmt-space": JUJU_SPACE_NAME},
        )
        bootstrapped = True
    else:
        click.echo(
            f"Juju controller '{MAAS_CONTROLLER}' already exists; skipping bootstrap."
        )

    click.echo(f"Switching to Juju controller '{MAAS_CONTROLLER}'.")
    juju.cli("switch", MAAS_CONTROLLER, include_model=False)

    if bootstrapped:
        click.echo("Waiting for controller machines to report ready status.")
        _wait_for_controller_ready(juju)

    return bootstrapped


def _is_already_exists_error(exc: jubilant.CLIError) -> bool:
    message = _format_juju_error(exc).lower()
    return "already exists" in message


def _create_nodes_impl(
    ctx_obj: dict[str, str],
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

    _get_lxd_vm_host_id(ctx_obj["admin"], ctx_obj["vmhost"])  # ensure host exists
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
    _ensure_maas_tag(ctx_obj["admin"], CEPHTOOLS_TAG)
    hostname_to_system_id = _tag_maas_machines(
        ctx_obj["admin"], hostnames, CEPHTOOLS_TAG
    )
    _tag_data_disks(ctx_obj["admin"], hostnames, hostname_to_system_id, tag="osd")


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
        return CleanupPhaseResult(phase, "failed", str(exc))

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
    admin: str, vmhost: str, *, dry_run: bool = False
) -> CleanupPhaseResult:
    phase = f"delete vm host {vmhost}"
    if dry_run:
        return CleanupPhaseResult(
            phase,
            "ok",
            f"dry-run: would delete MAAS VM host {vmhost}",
        )

    if shutil.which("maas") is None:
        return CleanupPhaseResult(phase, "skipped", "maas command not found")

    try:
        host_id = _get_lxd_vm_host_id(admin, vmhost)
    except click.ClickException as exc:
        detail = str(exc)
        if _message_indicates_not_found(detail):
            return CleanupPhaseResult(phase, "skipped", f"VM host {vmhost} not found")
        return CleanupPhaseResult(phase, "failed", detail)
    except subprocess.CalledProcessError as exc:
        return CleanupPhaseResult(phase, "failed", _format_process_error(exc))

    try:
        run(f"maas {admin} vm-host delete {host_id}")
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
        return CleanupPhaseResult(phase, "failed", str(exc))

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
        return CleanupPhaseResult(phase, "skipped", "No matching apt packages installed")

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
    "--admin", default=DEFAULTS["admin"], show_default=True, help="MAAS admin user"
)
@click.option(
    "--admin-pw",
    default=DEFAULTS["admin_pw"],
    show_default=True,
    help="MAAS admin password",
)
@click.option(
    "--admin-mail",
    default=DEFAULTS["admin_mail"],
    show_default=True,
    help="MAAS admin email",
)
@click.option(
    "--maas-version",
    default=DEFAULTS["maas_version"],
    show_default=True,
    help="MAAS PPA version, e.g. 3.7",
)
@click.option(
    "--lxdbridge",
    default=DEFAULTS["lxdbridge"],
    show_default=True,
    help="LXD bridge name",
)
@click.option(
    "--vmhost",
    default=DEFAULTS["vmhost"],
    show_default=True,
    help="VM host name in MAAS",
)
@click.pass_context
def cli(ctx, admin, admin_pw, admin_mail, maas_version, lxdbridge, vmhost):
    ctx.ensure_object(dict)
    ctx.obj.update(
        admin=admin,
        admin_pw=admin_pw,
        admin_mail=admin_mail,
        maas_version=maas_version,
        lxdbridge=lxdbridge,
        vmhost=vmhost,
        ip=primary_ip(),
    )
    ctx.obj["maas_url"] = f"http://{ctx.obj['ip']}:5240/MAAS"


@cli.command(
    "install-deps",
    help="Install MAAS from debs with PostgreSQL, plus lxd, terraform, and terragrunt.",
)
@click.pass_context
def install_deps(ctx):
    install_maas_deb(ctx.obj["maas_version"])
    ensure_snap("lxd")
    ensure_snap("terraform", classic=True)
    ensure_terragrunt()
    lxd_ready()
    click.echo("deps installed.")


@cli.command("lxd-init", help="Initialize LXD and tweak bridge.")
@click.pass_context
def lxd_init_cmd(ctx):
    lxd_init_impl(ctx.obj["ip"], ctx.obj["admin_pw"], ctx.obj["lxdbridge"])
    verify_lxd(ctx.obj["lxdbridge"])
    lxd_warmup()
    click.echo("lxd ready.")


@cli.command(
    "maas-init",
    help="Configure PostgreSQL-backed MAAS, create admin, and login.",
)
@click.pass_context
def maas_init_cmd(ctx):
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
    configure_maas_bind9_ipv4()
    click.echo("maas bind9 configured.")


@cli.command("register-vm-host", help="Register local LXD as MAAS VM host.")
@click.pass_context
def register_vm_host(ctx):
    register_lxd_vmhost_impl(
        ctx.obj["admin"], ctx.obj["vmhost"], ctx.obj["ip"], ctx.obj["admin_pw"]
    )
    import_boot_resources(ctx.obj["admin"])
    _wait_for_vm_host_architecture(
        ctx.obj["admin"], ctx.obj["vmhost"], REQUIRED_BOOT_ARCHITECTURE
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
    cidr, gw = route_info(ctx.obj["lxdbridge"])
    sid, fabric_id, vlan_id, rack_sysid = maas_subnet_ids(ctx.obj["admin"], cidr)
    update_subnet_gateway(ctx.obj["admin"], sid, gw)
    start_ip, end_ip = create_dynamic_iprange(ctx.obj["admin"], sid, cidr)
    enable_vlan_dhcp(ctx.obj["admin"], fabric_id, vlan_id, rack_sysid)
    click.echo(f"network configured on {ctx.obj['lxdbridge']} ({cidr}, gw {gw}).")
    space_id = create_space(ctx.obj["admin"], JUJU_SPACE_NAME)
    assign_space_to_vlan(ctx.obj["admin"], fabric_id, vlan_id, space_id)
    click.echo(f"space '{JUJU_SPACE_NAME}' ({space_id}) created and assigned to VLAN.")
    ext_cidr, ext_gw = lxd_network_cidr_and_gateway(EXT_LXD_NETWORK)
    ext_sid, ext_fabric_id, ext_vlan_id, ext_rack_sysid = maas_subnet_ids(
        ctx.obj["admin"], ext_cidr
    )
    update_subnet_gateway(ctx.obj["admin"], ext_sid, ext_gw)
    ext_start_ip, ext_end_ip = create_dynamic_iprange(
        ctx.obj["admin"], ext_sid, ext_cidr
    )
    enable_vlan_dhcp(ctx.obj["admin"], ext_fabric_id, ext_vlan_id, ext_rack_sysid)
    click.echo(f"network configured on {EXT_LXD_NETWORK} ({ext_cidr}, gw {ext_gw}).")
    ext_space_id = create_space(ctx.obj["admin"], EXTERNAL_SPACE_NAME)
    assign_space_to_vlan(ctx.obj["admin"], ext_fabric_id, ext_vlan_id, ext_space_id)
    click.echo(
        f"space '{EXTERNAL_SPACE_NAME}' ({ext_space_id}) created and assigned to VLAN."
    )
    network_yaml = "\n".join(
        [
            "network:",
            f"  bridge: {ctx.obj['lxdbridge']}",
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
            f"    bridge: {EXT_LXD_NETWORK}",
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
    click.echo("Terragrunt apply completed; MAAS will reconcile VM nodes.")


@cli.command(
    "destroy-nodes",
    help="Destroy MAAS VMs previously created by ensure-nodes using saved Terragrunt inputs.",
)
@click.pass_context
def destroy_nodes(ctx):
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
    help="Do not destroy Terragrunt-managed nodes.",
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
    keep_state: bool,
    purge_installed: bool,
) -> None:
    preserve_flags = {
        "--keep-nodes": keep_nodes,
        "--keep-controller": keep_controller,
        "--keep-vm-host": keep_vm_host,
        "--keep-lxd-instances": keep_lxd_instances,
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

    results: list[CleanupPhaseResult] = []
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

    if keep_controller:
        results.append(
            CleanupPhaseResult(
                f"kill controller {MAAS_CONTROLLER}",
                "skipped",
                "preserved by --keep-controller",
            )
        )
    else:
        results.append(
            _cleanup_kill_controller(MAAS_CONTROLLER, dry_run=True)
            if dry_run
            else _cleanup_kill_controller(MAAS_CONTROLLER)
        )

    if keep_vm_host:
        results.append(
            CleanupPhaseResult(
                f"delete vm host {ctx.obj['vmhost']}",
                "skipped",
                "preserved by --keep-vm-host",
            )
        )
    else:
        results.append(
            _cleanup_delete_vm_host(
                ctx.obj["admin"],
                ctx.obj["vmhost"],
                dry_run=True,
            )
            if dry_run
            else _cleanup_delete_vm_host(
                ctx.obj["admin"],
                ctx.obj["vmhost"],
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
        if keep_nodes:
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
        results.extend(
            [
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
    help="Install Juju, verify LXD/MAAS, write cred.yaml, add cloud/cred, bootstrap.",
)
@click.pass_context
def juju_init(ctx):
    # health checks before creds
    verify_lxd(ctx.obj["lxdbridge"])
    verify_maas(ctx.obj["admin"])
    _wait_for_vm_host_architecture(
        ctx.obj["admin"], ctx.obj["vmhost"], REQUIRED_BOOT_ARCHITECTURE
    )
    # juju install + creds/cloud
    ensure_snap("juju")
    api_key = maas_api_key(ctx.obj["admin"])
    write_cred_yaml(api_key)
    bootstrapped = juju_onboard()
    if bootstrapped:
        click.echo("juju initialized and controller bootstrapped.")
    else:
        click.echo("juju already initialized.")


@cli.command(
    "install",
    help="Run all installation steps: install-deps, lxd-init, maas-init, register-vm-host, configure-network, juju-init, create-model.",
)
@click.pass_context
def install(ctx):
    """Run all testenv installation steps in sequence."""
    click.echo("Starting full testenv installation...")

    click.echo("\n=== Step 1/7: Installing dependencies ===")
    ctx.invoke(install_deps)

    click.echo("\n=== Step 2/7: Initializing LXD ===")
    ctx.invoke(lxd_init_cmd)

    click.echo("\n=== Step 3/7: Initializing MAAS ===")
    ctx.invoke(maas_init_cmd)

    click.echo("\n=== Step 4/7: Registering VM host ===")
    ctx.invoke(register_vm_host)

    click.echo("\n=== Step 5/7: Configuring network ===")
    ctx.invoke(configure_network)

    click.echo("\n=== Step 6/7: Initializing Juju ===")
    ctx.invoke(juju_init)

    click.echo("\n=== Step 7/7: Creating Juju model ===")
    _ensure_juju_model(CEPHTOOLS_MODEL, constraint=f"tags={CEPHTOOLS_TAG}")
    click.echo(
        f"Juju model '{CEPHTOOLS_MODEL}' ensured with constraint tags={CEPHTOOLS_TAG}."
    )

    click.echo("\n=== Installation complete! ===")
    click.echo(f"MAAS URL: {ctx.obj['maas_url']}")
    click.echo(f"Admin user: {ctx.obj['admin']}")
    click.echo("You can now use 'juju status' to check your controller.")


def main():
    try:
        cli(obj={})
    except subprocess.CalledProcessError as e:
        print(e.stdout)
        print(e.stderr, file=sys.stderr)
        sys.exit(e.returncode)


if __name__ == "__main__":
    main()
