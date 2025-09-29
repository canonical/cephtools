#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import shlex
import socket
import subprocess
import sys
import time
from ipaddress import ip_network
from pathlib import Path

import click

# ---- defaults via env ------------------------------------------------------
DEFAULTS = dict(
    maas_ch=os.getenv("MAAS_CH", "3.6/stable"),
    admin=os.getenv("MAAS_ADMIN", "admin"),
    admin_pw=os.getenv("MAAS_ADMIN_PW", "maaspass"),
    admin_mail=os.getenv("MAAS_ADMIN_MAIL", "admin@example.com"),
    lxdbridge=os.getenv("LXDBRIDGE", "lxdbr0"),
    vmhost=os.getenv("VMHOST", "local-lxd"),
)


# ---- small helpers ---------------------------------------------------------
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
        stderr=subprocess.PIPE,
        shell=shell,
    )


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


def ensure_snap(name, channel=None):
    out = run("snap list", check=True)
    if any(line.split()[0] == name for line in out.stdout.splitlines()[1:]):
        return
    cmd = f"sudo snap install {name}"
    if channel:
        cmd += f" --channel={channel}"
    run(cmd)


def lxd_ready():
    try:
        run("sudo lxd waitready", check=True)
    except subprocess.CalledProcessError as e:
        print(e.stderr)


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
    status = run("sudo maas status || true", shell=True).stdout.lower()
    regiond_ok = re.search(r'regiond\s+enabled\s+active', status)
    rackd_ok = re.search(r'rackd\s+enabled\s+active', status)
    if not regiond_ok or not rackd_ok:
        raise RuntimeError("MAAS services not running (regiond/rackd must be enabled and active)")
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


def import_boot_resources(admin):
    run(f'maas "{admin}" boot-resources import')


def route_info(lxdbridge):
    out = run(f"ip -j r s dev {lxdbridge}")
    routes = json.loads(out.stdout)
    
    for route in routes:
        dst = route.get("dst")
        prefsrc = route.get("prefsrc")
        
        if dst and "/" in dst and prefsrc:
            return dst, prefsrc
    
    raise RuntimeError(f"could not derive CIDR or gateway from routes: {routes}")


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
    return start_ip, end_ip


def enable_vlan_dhcp(admin, fabric_id, vlan_id, rack_sysid):
    run(
        f"maas {admin} vlan update {fabric_id} {vlan_id} dhcp_on=true primary_rack={rack_sysid}"
    )


def write_cloud_yaml(ip):
    Path("cloud.yaml").write_text(
        "clouds:\n"
        "  maas-cloud:\n"
        "    type: maas\n"
        "    auth-types: [oauth1]\n"
        f"    endpoint: http://{ip}:5240/MAAS\n"
    )


def write_cred_yaml(api_key):
    Path("cred.yaml").write_text(
        "credentials:\n"
        "  maas-cloud:\n"
        "    admin:\n"
        "      auth-type: oauth1\n"
        f"      maas-oauth: {api_key}\n"
    )


def juju_onboard():
    run("juju add-cloud maas-cloud cloud.yaml || true", shell=True)
    run("juju add-credential maas-cloud -f cred.yaml --client || true", shell=True)
    run("juju bootstrap maas-cloud maas-controller || true", shell=True)


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


@cli.command("install-deps", help="Install snaps: maas, maas-test-db, lxd.")
@click.pass_context
def install_deps(ctx):
    ensure_snap("maas", channel=ctx.obj["maas_ch"])
    ensure_snap("maas-test-db")
    ensure_snap("lxd")
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
    create_dynamic_iprange(ctx.obj["admin"], sid, cidr)
    enable_vlan_dhcp(ctx.obj["admin"], fabric_id, vlan_id, rack_sysid)
    click.echo(f"network configured on {ctx.obj['lxdbridge']} ({cidr}, gw {gw}).")


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


def main():
    try:
        cli(obj={})
    except subprocess.CalledProcessError as e:
        print(e.stdout)
        print(e.stderr, file=sys.stderr)
        sys.exit(e.returncode)


if __name__ == "__main__":
    main()
