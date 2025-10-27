#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import platform
import shlex
import shutil
import socket
import subprocess
import sys
import time
from ipaddress import ip_interface, ip_network
from pathlib import Path

import click
import jubilant
from cephtools.config import (
    load_cephtools_config,
    load_vmaas_defaults,
)
from cephtools.state import get_state_file
from cephtools.terraform import terraform_root_candidates
from cephtools.testflinger import (
    read_vmaas_cloud_config,
    read_vmaas_credentials,
    read_vmaas_network_config,
)

# ---- defaults from configuration -----------------------------------------
DEFAULTS = load_vmaas_defaults()

TERRAGRUNT_VERSION = "v0.89.3"
CEPHTOOLS_TAG = DEFAULTS["maas_tag"]
CEPHTOOLS_MODEL = load_cephtools_config(ensure=True)["juju_model"]
MAAS_CONTROLLER = "maas-controller"
EXT_LXD_NETWORK = "ext"
EXTERNAL_SPACE_NAME = "external"
JUJU_SPACE_NAME = "jujuspace"


def run(cmd, check=True, shell=False, quiet=False):
    if not quiet:
        print(f"+ {cmd}")
    if not shell:
        cmd = shlex.split(cmd)
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        shell=shell,
    )


def _format_juju_error(exc: jubilant.CLIError) -> str:
    stderr = (getattr(exc, "stderr", "") or "").strip()
    stdout = (getattr(exc, "output", "") or "").strip()
    if stderr:
        return stderr
    if stdout:
        return stdout
    return f"exit code {getattr(exc, 'returncode', 'unknown')}"


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
        raise click.ClickException(
            "Terragrunt outputs did not include vm_hostnames."
        )

    hostnames = hostnames_value["value"]
    if not isinstance(hostnames, list):
        raise click.ClickException(
            "Terragrunt vm_hostnames output must be a list."
        )

    return [str(hostname) for hostname in hostnames]


def _ensure_maas_tag(admin: str, tag: str) -> None:
    result = run(f"maas {admin} tags read")
    try:
        tags = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise click.ClickException(
            "Failed to parse MAAS tags output as JSON."
        ) from exc

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


def _tag_data_disks(admin: str, hostnames: list[str], hostname_to_system_id: dict[str, str], *, tag: str) -> None:
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


def ensure_snap(name, channel=None, classic=False):
    out = run("snap list", check=True)
    if any(line.split()[0] == name for line in out.stdout.splitlines()[1:]):
        return
    parts = ["sudo", "snap", "install", name]
    if channel:
        parts.append(f"--channel={channel}")
    if classic:
        parts.append("--classic")
    run(" ".join(parts))


def ensure_terragrunt(version=TERRAGRUNT_VERSION, bin_dir="/usr/local/bin"):
    bin_path = Path(bin_dir) / "terragrunt"
    if bin_path.exists() or shutil.which("terragrunt"):
        return

    system = platform.system().lower()
    if not system.startswith("linux"):
        raise RuntimeError("Terragrunt installer currently supports only Linux hosts")
    system = "linux"

    machine = platform.machine().lower()
    arch_map = {
        "x86_64": "amd64",
        "amd64": "amd64",
        "aarch64": "arm64",
        "arm64": "arm64",
    }
    arch = arch_map.get(machine)
    if arch is None:
        raise RuntimeError(f"Unsupported architecture for terragrunt: {platform.machine()}")

    terragrunt_bin = f"terragrunt_{system}_{arch}"
    terragrunt_url = (
        f"https://github.com/gruntwork-io/terragrunt/releases/download/{version}/{terragrunt_bin}"
    )

    run(f"curl -fsSL -o {terragrunt_bin} {terragrunt_url}")
    run(f"chmod +x {terragrunt_bin}")
    run(f"sudo mv {terragrunt_bin} {bin_path}")


def lxd_ready():
    try:
        run("sudo lxd waitready", check=True)
    except subprocess.CalledProcessError as e:
        print(e.stderr)


def ensure_lxd_network(name: str, *, ipv4_address: str | None = None) -> None:
    nets = json.loads(run("lxc query /1.0/networks").stdout)
    if f"/1.0/networks/{name}" in nets:
        return

    address_arg = ipv4_address if ipv4_address else "auto"
    run(
        f"lxc network create {name} ipv4.address={address_arg} ipv4.nat=true ipv6.address=none"
    )


def lxd_init_impl(ip, admin_pw, lxdbridge):
    run("sudo snap set lxd daemon.user.group=adm")
    run(
        f"sudo lxd init --auto --trust-password={shlex.quote(admin_pw)} "
        f"--network-address={ip} --network-port=8443 || true",
        shell=True,
    )
    run("lxc config set core.https_address :8443 || true", shell=True)
    for k, v in [("dns.mode", "none"), ("ipv4.dhcp", "false"), ("ipv6.dhcp", "false")]:
        run(f"lxc network set {lxdbridge} {k}={v} || true", shell=True)
    ensure_lxd_network(EXT_LXD_NETWORK)
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
    ext_net = json.loads(run(f"lxc query /1.0/networks/{EXT_LXD_NETWORK}").stdout)
    if ext_net.get("managed") is not True:
        raise RuntimeError(f"Network {EXT_LXD_NETWORK} is not managed")


def maas_init_impl(maas_url, admin, admin_pw, admin_mail):
    try:
        run(
            "sudo maas init region+rack --database-uri maas-test-db:/// "
            f"--admin-username {admin} --admin-password {admin_pw} "
            f"--admin-email {admin_mail} --maas-url {maas_url}"
        )
        time.sleep(10)
    except subprocess.CalledProcessError as e:
        print(e.stderr.strip())
    try:
        run(
            f"sudo maas createadmin --username {admin} --password {admin_pw} --email {admin_mail}"
        )
    except subprocess.CalledProcessError as e:
        print(e.stderr.strip())


def maas_api_key(admin) -> str:
    out = run(f"sudo maas apikey --username {admin}")
    return out.stdout.strip()


def maas_login(maas_url, admin, api_key):
    run(f'maas login "{admin}" "{maas_url}" "{api_key}"')


def verify_maas(admin):
    import re

    status = run("sudo maas status").stdout.lower()
    regiond_ok = re.search(r"regiond\s+enabled\s+active", status)
    rackd_ok = re.search(r"rackd\s+enabled\s+active", status)
    if not regiond_ok or not rackd_ok:
        raise RuntimeError(
            "MAAS services not running (regiond/rackd must be enabled and active)"
        )
    _ = run(f"maas {admin} boot-resources read").stdout


def register_lxd_vmhost_impl(admin, vmhost, ip, admin_pw):
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
    """
    Return the unique CPU arches from MAAS boot-resources JSON.
    Looks at the 'architecture' field and takes the part before '/'.
    """
    arches = set()
    for item in resources:
        arches.add(item.get("architecture"))
    return arches


def import_boot_resources(admin):
    run(f'maas "{admin}" boot-resources import')
    time.sleep(10)
    # read boot and loop until we have amd64/generic arch
    for _ in range(20):
        out = run(f"maas {admin} boot-resources read").stdout
        resources = json.loads(out)
        arches = extract_arches(resources)
        if "amd64/generic" in arches:
            return
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
    raise click.ClickException(
        f"VM host '{vmhost}' not found in MAAS vm-hosts output."
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


def juju_onboard():
    cloud_path = get_state_file("cloud.yaml")
    cred_path = get_state_file("cred.yaml")
    juju = jubilant.Juju()
    try:
        juju.cli(
            "add-cloud",
            "maas-cloud",
            str(cloud_path),
            "--client",
            include_model=False,
        )
        juju.cli(
            "add-credential",
            "maas-cloud",
            "-f",
            str(cred_path),
            "--client",
            include_model=False,
        )
        time.sleep(2)
        juju.bootstrap(
            "maas-cloud",
            MAAS_CONTROLLER,
            bootstrap_constraints={"spaces": JUJU_SPACE_NAME},
            config={"juju-mgmt-space": JUJU_SPACE_NAME},
        )
        juju.cli("switch", MAAS_CONTROLLER, include_model=False)
    except jubilant.CLIError as exc:
        message = _format_juju_error(exc)
        raise click.ClickException(
            f"Failed to bootstrap Juju controller: {message}"
        ) from exc

    time.sleep(10)
    # poll controller status until ready
    for _ in range(20):
        controllers_json = juju.cli(
            "controllers",
            "--format",
            "json",
            include_model=False,
        )
        js = json.loads(controllers_json or "{}")
        total_ctrl_machines = sum(
            c.get("controller-machines", {}).get("Total", 0)
            for c in js.get("controllers", {}).values()
        )
        if total_ctrl_machines > 0:
            return
        time.sleep(6)
    raise Exception("juju controller machines not ready after timeout")


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

    vm_host_id = _get_lxd_vm_host_id(ctx_obj["admin"], ctx_obj["vmhost"])

    clouds = read_vmaas_cloud_config()
    try:
        maas_cloud = clouds["maas-cloud"]
        maas_api_url = maas_cloud["endpoint"]
    except KeyError as exc:
        raise click.ClickException(
            "cloud.yaml is missing required maas-cloud endpoint."
        ) from exc

    credentials = read_vmaas_credentials()
    try:
        maas_api_key = credentials["maas-cloud"]["admin"]["maas-oauth"]
    except KeyError as exc:
        raise click.ClickException(
            "cred.yaml is missing maas-cloud admin credentials."
        ) from exc

    network = read_vmaas_network_config()
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

    var_args = [
        f"-var {shlex.quote(f'maas_api_url={maas_api_url}')}",
        f"-var {shlex.quote(f'maas_api_key={maas_api_key}')}",
        f"-var {shlex.quote(f'lxd_vm_host_id={vm_host_id}')}",
        f"-var {shlex.quote(f'vm_data_disk_size={vm_data_disk_size}')}",
        f"-var {shlex.quote(f'vm_data_disk_count={vm_data_disk_count}')}",
        f"-var {shlex.quote(f'vm_count={vm_count}')}",
        f"-var {shlex.quote(f'primary_subnet_cidr={primary_subnet_cidr}')}",
        f"-var {shlex.quote(f'external_subnet_cidr={external_subnet_cidr}')}",
    ]
    terragrunt_args = [
        "terragrunt",
        "apply",
        "-auto-approve",
        "-parallelism=1",
        *var_args,
    ]
    terragrunt_cmd = " ".join(terragrunt_args)
    run(
        f"cd {shlex.quote(str(terragrunt_dir))} && {terragrunt_cmd}",
        shell=True,
    )

    hostnames = _terragrunt_vm_hostnames(terragrunt_dir)
    _ensure_maas_tag(ctx_obj["admin"], CEPHTOOLS_TAG)
    hostname_to_system_id = _tag_maas_machines(ctx_obj["admin"], hostnames, CEPHTOOLS_TAG)
    _tag_data_disks(ctx_obj["admin"], hostnames, hostname_to_system_id, tag="osd")


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
    "--maas-ch",
    default=DEFAULTS["maas_ch"],
    show_default=True,
    help="Snap channel for MAAS",
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
def cli(ctx, admin, admin_pw, admin_mail, maas_ch, lxdbridge, vmhost):
    ctx.ensure_object(dict)
    ctx.obj.update(
        admin=admin,
        admin_pw=admin_pw,
        admin_mail=admin_mail,
        maas_ch=maas_ch,
        lxdbridge=lxdbridge,
        vmhost=vmhost,
        ip=primary_ip(),
    )
    ctx.obj["maas_url"] = f"http://{ctx.obj['ip']}:5240/MAAS"


@cli.command(
    "install-deps",
    help="Install snaps and tools: maas, maas-test-db, lxd, terraform, terragrunt.",
)
@click.pass_context
def install_deps(ctx):
    ensure_snap("maas", channel=ctx.obj["maas_ch"])
    ensure_snap("maas-test-db")
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
    click.echo("lxd ready.")


@cli.command(
    "maas-init", help="Initialize MAAS (region+rack), create admin, and login."
)
@click.pass_context
def maas_init_cmd(ctx):
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
    click.echo("maas initialized and logged in.")
    # Write cloud.yaml now; cred.yaml later in juju-init after health checks again.
    write_cloud_yaml(ctx.obj["ip"])
    click.echo("cloud.yaml written.")


@cli.command("register-vm-host", help="Register local LXD as MAAS VM host.")
@click.pass_context
def register_vm_host(ctx):
    register_lxd_vmhost_impl(
        ctx.obj["admin"], ctx.obj["vmhost"], ctx.obj["ip"], ctx.obj["admin_pw"]
    )
    import_boot_resources(ctx.obj["admin"])
    click.echo("vm host registered and boot resources import kicked off.")


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
    ext_sid, ext_fabric_id, ext_vlan_id, ext_rack_sysid = maas_subnet_ids(ctx.obj["admin"], ext_cidr)
    update_subnet_gateway(ctx.obj["admin"], ext_sid, ext_gw)
    ext_start_ip, ext_end_ip = create_dynamic_iprange(ctx.obj["admin"], ext_sid, ext_cidr)
    enable_vlan_dhcp(ctx.obj["admin"], ext_fabric_id, ext_vlan_id, ext_rack_sysid)
    click.echo(f"network configured on {EXT_LXD_NETWORK} ({ext_cidr}, gw {ext_gw}).")
    ext_space_id = create_space(ctx.obj["admin"], EXTERNAL_SPACE_NAME)
    assign_space_to_vlan(ctx.obj["admin"], ext_fabric_id, ext_vlan_id, ext_space_id)
    click.echo(f"space '{EXTERNAL_SPACE_NAME}' ({ext_space_id}) created and assigned to VLAN.")
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
    help="Ensure the desired set of MAAS VMs exist using terragrunt and VMaaS configuration files.",
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
    click.echo(
        "Terragrunt apply completed; MAAS will reconcile VM nodes."
    )


@cli.command(
    "juju-init",
    help="Install Juju, verify LXD/MAAS, write cred.yaml, add cloud/cred, bootstrap.",
)
@click.pass_context
def juju_init(ctx):
    # health checks before creds
    verify_lxd(ctx.obj["lxdbridge"])
    verify_maas(ctx.obj["admin"])
    # juju install + creds/cloud
    ensure_snap("juju")
    api_key = maas_api_key(ctx.obj["admin"])
    write_cred_yaml(api_key)
    juju_onboard()
    click.echo("juju initialized and controller bootstrapped.")


@cli.command(
    "install",
    help="Run all installation steps: install-deps, lxd-init, maas-init, register-vm-host, configure-network, juju-init, create-model.",
)
@click.pass_context
def install(ctx):
    """Run all vmaas installation steps in sequence."""
    click.echo("Starting full vmaas installation...")

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
