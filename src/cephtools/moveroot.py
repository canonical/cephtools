"""Move a reserved host's root filesystem onto another disk over SSH.

The work happens in two remote steps with a reboot in between:

* ``prepare`` partitions the new disk, copies the running root and ESP onto
  it, makes it bootable (fstab, GRUB, initramfs) while keeping the firmware
  BootOrder, points the old ESP's GRUB stub at the new root and records the
  filesystem UUIDs in a state file on the new root.
* ``finish`` runs after the reboot, checks that ``/`` and ``/boot/efi`` really
  come from the new disk and only then wipes the old disk. The old disk is
  found again by its root filesystem UUID because kernel device names can
  change across a reboot.

Each step runs as a transient systemd unit on the host so that a dropped SSH
connection does not interrupt it; the local side follows the unit's journal
and reconnects when it can.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import re
import shlex
import subprocess
import time
from typing import Callable

import click


Runner = Callable[..., subprocess.CompletedProcess]

REMOTE_SCRIPT_DIR = "/run/cephtools-moveroot"
STATE_FILE = "/var/lib/cephtools/moveroot.env"
UNIT_PREFIX = "cephtools-moveroot"
POLL_MARKER = "@@cephtools-moveroot-status@@"
CURSOR_PREFIX = "-- cursor: "
DEFAULT_REBOOT_TIMEOUT = 2700
DEFAULT_POLL_INTERVAL = 5.0
DEFAULT_DISCONNECT_TIMEOUT = 900.0
REBOOT_POLL_INTERVAL = 10.0
SSH_RETRY_SECONDS = 60.0
SSH_RETRY_INTERVAL = 5.0
_DEVICE_SEGMENT = r"[A-Za-z0-9][A-Za-z0-9._:+@-]*"
DEVICE_RE = re.compile(rf"^/dev/{_DEVICE_SEGMENT}(/{_DEVICE_SEGMENT})*$")


# Shared shell helpers. The ERR trap only reports failures of the main shell
# so that probes inside command substitutions do not print spurious errors.
_COMMON_SH = r"""
set -Eeuo pipefail
trap '[ "$BASH_SUBSHELL" -eq 0 ] && printf "moveroot: ERROR: command failed at line %s: %s\n" "$LINENO" "$BASH_COMMAND" >&2' ERR

log() { printf 'moveroot: %s\n' "$*"; }
warn() { printf 'moveroot: WARNING: %s\n' "$*" >&2; }
die() { printf 'moveroot: ERROR: %s\n' "$*" >&2; exit 1; }

require_commands() {
    local missing=() cmd
    for cmd in "$@"; do
        command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
    done
    [ "${#missing[@]}" -eq 0 ] || die "missing required commands: ${missing[*]}"
}

# Print the whole disk (/dev/...) that holds the given partition.
parent_disk() {
    local pk
    pk=$(lsblk -dno PKNAME "$1") || return 1
    [ -n "$pk" ] || return 1
    printf '/dev/%s\n' "$pk"
}

# Mount points (including [SWAP]) of a disk and everything on it.
disk_mountpoints() {
    lsblk -lnpo MOUNTPOINT "$1" | awk 'NF'
}

# Anything stacked on a disk that is not a plain partition (LVM, RAID, ...).
non_partition_children() {
    lsblk -lnpo NAME,TYPE "$1" | awk 'NR > 1 && $2 != "part" { print $1 }'
}

partitions_of() {
    lsblk -lnpo NAME,TYPE "$1" | awk 'NR > 1 && $2 == "part" { print $1 }'
}
"""

_PREPARE_SH = r"""
ESP_MIB=512
MNT=/mnt/cephtools-moveroot
BOOT_ORDER_FILE=/var/lib/cephtools/moveroot-bootorder.orig
SWAP_FILES=()
EXCLUDES=()
BOOT_ORDER_TOUCHED=0

# Kernel name of partition N on a disk: sdb -> sdb1, nvme0n1 -> nvme0n1p1.
part_path() {
    case "$1" in
        *[0-9]) printf '%sp%s\n' "$1" "$2" ;;
        *) printf '%s%s\n' "$1" "$2" ;;
    esac
}

wait_for_block() {
    local _
    for _ in $(seq 1 30); do
        [ -b "$1" ] && return 0
        sleep 1
        udevadm settle || true
    done
    die "$1 did not appear"
}

# Merge the BootOrder grub-install left behind back into the original order:
# keep the original sequence, put entries grub-install created where the ones
# it deleted used to be (or at the end), so network boot stays first.
compute_boot_order() {
    local orig=$1 after=$2 entry inserted=0
    local -a orig_list=() after_list=() added=() result=()
    IFS=, read -ra orig_list <<<"$orig"
    IFS=, read -ra after_list <<<"$after"
    for entry in "${after_list[@]}"; do
        [[ ",$orig," == *",$entry,"* ]] || added+=("$entry")
    done
    for entry in "${orig_list[@]}"; do
        if [[ ",$after," == *",$entry,"* ]]; then
            result+=("$entry")
        elif [ "$inserted" -eq 0 ]; then
            result+=("${added[@]}")
            inserted=1
        fi
    done
    [ "$inserted" -eq 1 ] || result+=("${added[@]}")
    local IFS=,
    printf '%s\n' "${result[*]}"
}

restore_boot_order() {
    local orig current wanted
    orig=$(cat "$BOOT_ORDER_FILE" 2>/dev/null || true)
    BOOT_ORDER_TOUCHED=0
    [ -n "$orig" ] || return 0
    current=$(efibootmgr | sed -n 's/^BootOrder: //p')
    wanted=$(compute_boot_order "$orig" "$current")
    if [ -n "$wanted" ] && [ "$wanted" != "$current" ]; then
        log "Restoring EFI BootOrder to $wanted (grub-install had set $current)"
        efibootmgr -q -o "$wanted"
    fi
}

# Rewrite the / and /boot/efi sources of an fstab, whatever form they had.
rewrite_fstab() {
    awk -v root="UUID=$3" -v esp="UUID=$4" '
        /^[[:space:]]*(#|$)/ { print; next }
        $2 == "/" { $1 = root; seen_root = 1 }
        $2 == "/boot/efi" { $1 = esp; seen_esp = 1 }
        { print }
        END { if (!(seen_root && seen_esp)) exit 3 }
    ' "$1" > "$2"
}

# rsync exit code 24 means files vanished while copying a live system.
rsync_tolerant() {
    local rc=0
    rsync "$@" || rc=$?
    case "$rc" in
        0|24) return 0 ;;
        *) die "rsync failed with exit code $rc" ;;
    esac
}

bind_mount() {
    mkdir -p "$2"
    mount --bind "$1" "$2"
    mount --make-slave "$2"
}

umount_chroot() {
    local sub
    for sub in run sys/firmware/efi/efivars sys proc dev/pts dev; do
        if mountpoint -q "$MNT/$sub"; then
            umount "$MNT/$sub"
        fi
    done
}

cleanup() {
    if [ "$BOOT_ORDER_TOUCHED" -eq 1 ]; then
        restore_boot_order || warn "could not restore the EFI BootOrder"
    fi
    if mountpoint -q "$MNT"; then
        umount -R "$MNT" 2>/dev/null || umount -Rl "$MNT" || true
    fi
}

# Warn when a loadable driver needed to reach the disk is missing from an initrd.
check_initramfs_drivers() {
    local initrd=$1 disk=$2 listing path mod pattern
    if [ ! -f "$initrd" ]; then
        warn "$initrd not found; skipping the driver check"
        return 0
    fi
    if ! listing=$(lsinitramfs "$initrd" 2>/dev/null); then
        warn "could not list $initrd; skipping the driver check"
        return 0
    fi
    path=$(readlink -f "/sys/class/block/${disk##*/}/device") || return 0
    while [ -n "$path" ] && [ "$path" != /sys ] && [ "$path" != /sys/devices ]; do
        if [ -e "$path/driver/module" ]; then
            mod=$(basename "$(readlink -f "$path/driver/module")")
            if [ -e "/sys/module/$mod/initstate" ]; then
                pattern="/$(printf '%s' "$mod" | sed 's/[-_]/[-_]/g')\.ko"
                if ! grep -Eq "$pattern" <<<"$listing"; then
                    warn "driver $mod for $disk is not in $initrd; the host may not find its root after the reboot"
                fi
            fi
        fi
        path=${path%/*}
    done
}

preflight() {
    [ "$(id -u)" -eq 0 ] || die "must run as root"
    [ -d /sys/firmware/efi ] || die "only UEFI-booted hosts are supported"
    require_commands awk blkid blockdev chroot df efibootmgr fallocate findmnt \
        grub-install lsblk lsinitramfs mkfs.ext4 mkfs.vfat mkswap mountpoint \
        rsync sgdisk udevadm update-grub update-initramfs wipefs
    case "$(uname -m)" in
        x86_64) GRUB_TARGET=x86_64-efi ;;
        aarch64) GRUB_TARGET=arm64-efi ;;
        *) die "unsupported architecture $(uname -m)" ;;
    esac

    OLD_DISK=$(readlink -f "$OLD_DEV")
    NEW_DISK=$(readlink -f "$NEW_DEV")
    local disk
    for disk in "$OLD_DISK" "$NEW_DISK"; do
        [ -b "$disk" ] || die "$disk is not a block device"
        [ "$(lsblk -dno TYPE "$disk")" = disk ] || die "$disk is not a whole disk"
    done
    [ "$OLD_DISK" != "$NEW_DISK" ] || die "old and new device are the same disk ($OLD_DISK)"

    ROOT_PART=$(findmnt -no SOURCE /)
    [ "$(findmnt -no FSTYPE /)" = ext4 ] || die "/ must be an ext4 filesystem"
    [ "$(lsblk -dno TYPE "$ROOT_PART")" = part ] ||
        die "/ ($ROOT_PART) must be a plain partition; LVM, RAID and encryption are not supported"
    [ "$(parent_disk "$ROOT_PART")" = "$OLD_DISK" ] || die "/ ($ROOT_PART) is not on $OLD_DISK"
    if mountpoint -q /boot; then
        die "a separate /boot filesystem is not supported"
    fi
    mountpoint -q /boot/efi || die "/boot/efi is not mounted"
    ESP_PART=$(findmnt -no SOURCE /boot/efi)
    [ "$(parent_disk "$ESP_PART")" = "$OLD_DISK" ] || die "/boot/efi ($ESP_PART) is not on $OLD_DISK"
    [ -f /boot/efi/EFI/ubuntu/grub.cfg ] || die "/boot/efi/EFI/ubuntu/grub.cfg not found"

    local name mnt_point
    while read -r name mnt_point; do
        case "$mnt_point" in
            "" | / | /boot/efi) ;;
            *) die "$name is in use ($mnt_point); only / and /boot/efi may live on $OLD_DISK" ;;
        esac
    done < <(lsblk -lnpo NAME,MOUNTPOINT "$OLD_DISK")

    local busy
    busy=$(disk_mountpoints "$NEW_DISK")
    [ -z "$busy" ] || die "$NEW_DISK is in use (${busy//$'\n'/ })"
    busy=$(non_partition_children "$NEW_DISK")
    [ -z "$busy" ] || die "$NEW_DISK has holders (${busy//$'\n'/ }); refusing to touch it"
    if [ "$FORCE" != 1 ]; then
        [ -z "$(partitions_of "$NEW_DISK")" ] ||
            die "$NEW_DISK has partitions; pass --force to overwrite it"
        [ -z "$(wipefs -n "$NEW_DISK")" ] ||
            die "$NEW_DISK has filesystem or partition table signatures; pass --force to overwrite it"
    fi

    # Active swap files on / are recreated instead of copied.
    local swap_name swap_type _rest root_dev
    root_dev=$(stat -c %d /)
    while read -r swap_name swap_type _rest; do
        case "$swap_type" in
            file)
                if [ "$(stat -c %d "$swap_name")" = "$root_dev" ]; then
                    SWAP_FILES+=("$swap_name")
                    EXCLUDES+=("--exclude=$swap_name")
                fi
                ;;
            partition)
                [ "$(parent_disk "$swap_name" || true)" != "$OLD_DISK" ] ||
                    die "swap partition $swap_name on $OLD_DISK is not supported"
                ;;
        esac
    done < <(tail -n +2 /proc/swaps)

    local used need size
    used=$(df -B1 --output=used / | tail -n 1 | tr -d ' ')
    need=$((used + (ESP_MIB + 2048) * 1024 * 1024))
    size=$(blockdev --getsize64 "$NEW_DISK")
    [ "$size" -ge "$need" ] || die "$NEW_DISK is too small: $size bytes, need at least $need"

    OLD_ROOT_UUID=$(findmnt -no UUID /)
    OLD_ESP_UUID=$(findmnt -no UUID /boot/efi)
    OLD_ESP_PARTUUID=$(lsblk -dno PARTUUID "$ESP_PART")
    [ -n "$OLD_ROOT_UUID" ] || die "could not determine the UUID of /"

    log "Old disk $OLD_DISK: / on $ROOT_PART (UUID $OLD_ROOT_UUID), /boot/efi on $ESP_PART"
    log "New disk $NEW_DISK: $size bytes, $used bytes used on /"
    lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT "$OLD_DISK" "$NEW_DISK" || true
}

partition_new_disk() {
    local part
    log "Partitioning $NEW_DISK"
    for part in $(partitions_of "$NEW_DISK"); do
        wipefs -aq "$part"
    done
    wipefs -aq "$NEW_DISK"
    sgdisk --zap-all "$NEW_DISK" >/dev/null
    sgdisk -n "1:1M:+${ESP_MIB}M" -t 1:ef00 -c 1:efi2 -n 2:0:0 -t 2:8300 -c 2:root2 "$NEW_DISK" >/dev/null
    udevadm settle || true
    NEW_ESP=$(part_path "$NEW_DISK" 1)
    NEW_ROOT=$(part_path "$NEW_DISK" 2)
    wait_for_block "$NEW_ESP"
    wait_for_block "$NEW_ROOT"
    wipefs -aq "$NEW_ESP" "$NEW_ROOT"

    log "Creating filesystems on $NEW_ESP and $NEW_ROOT"
    mkfs.vfat -F 32 -n EFI2 "$NEW_ESP" >/dev/null
    mkfs.ext4 -q -F -L root2 "$NEW_ROOT"
    udevadm settle || true
    NEW_ESP_UUID=$(blkid -p -s UUID -o value "$NEW_ESP")
    NEW_ROOT_UUID=$(blkid -p -s UUID -o value "$NEW_ROOT")
    [ -n "$NEW_ESP_UUID" ] && [ -n "$NEW_ROOT_UUID" ] || die "could not read the new filesystem UUIDs"
}

copy_root() {
    if mountpoint -q "$MNT"; then
        die "$MNT is already a mount point"
    fi
    mkdir -p "$MNT"
    mount "$NEW_ROOT" "$MNT"
    log "Copying / to $NEW_ROOT (this can take a while)"
    rsync_tolerant -aAXH --numeric-ids --one-file-system --info=stats1 "${EXCLUDES[@]}" / "$MNT"/
    mkdir -p "$MNT/boot/efi"
    mount "$NEW_ESP" "$MNT/boot/efi"
    log "Copying /boot/efi to $NEW_ESP"
    rsync_tolerant -rt /boot/efi/ "$MNT/boot/efi/"

    local swap
    for swap in "${SWAP_FILES[@]}"; do
        log "Creating a fresh swap file $swap"
        mkdir -p "$MNT$(dirname "$swap")"
        fallocate -l "$(stat -c %s "$swap")" "$MNT$swap"
        chmod 0600 "$MNT$swap"
        mkswap "$MNT$swap" >/dev/null
    done
}

make_bootable() {
    log "Pointing /etc/fstab on the new root at $NEW_ROOT_UUID and $NEW_ESP_UUID"
    cp -p "$MNT/etc/fstab" "$MNT/etc/fstab.pre-moveroot"
    rewrite_fstab "$MNT/etc/fstab.pre-moveroot" "$MNT/etc/fstab" "$NEW_ROOT_UUID" "$NEW_ESP_UUID" ||
        die "/etc/fstab has no / and /boot/efi entries to rewrite"

    bind_mount /dev "$MNT/dev"
    bind_mount /dev/pts "$MNT/dev/pts"
    mount -t proc proc "$MNT/proc"
    bind_mount /sys "$MNT/sys"
    bind_mount /sys/firmware/efi/efivars "$MNT/sys/firmware/efi/efivars"
    bind_mount /run "$MNT/run"

    # Keep the first-seen BootOrder so a re-run after a failure restores it.
    mkdir -p "$(dirname "$BOOT_ORDER_FILE")"
    if [ ! -s "$BOOT_ORDER_FILE" ]; then
        efibootmgr | sed -n 's/^BootOrder: //p' > "$BOOT_ORDER_FILE"
    fi

    log "Installing GRUB to $NEW_ESP"
    BOOT_ORDER_TOUCHED=1
    chroot "$MNT" grub-install --target="$GRUB_TARGET" --efi-directory=/boot/efi \
        --bootloader-id=ubuntu --uefi-secure-boot
    restore_boot_order
    chroot "$MNT" update-grub
    log "Regenerating the initramfs on the new root"
    chroot "$MNT" update-initramfs -u -k all

    grep -q "$NEW_ROOT_UUID" "$MNT/boot/efi/EFI/ubuntu/grub.cfg" ||
        die "the GRUB stub on $NEW_ESP does not reference $NEW_ROOT_UUID"
    grep -q "root=UUID=$NEW_ROOT_UUID" "$MNT/boot/grub/grub.cfg" ||
        die "/boot/grub/grub.cfg on $NEW_ROOT does not boot root=UUID=$NEW_ROOT_UUID"
    check_initramfs_drivers "$MNT/boot/initrd.img-$(uname -r)" "$NEW_DISK"
    umount_chroot
}

# Whatever still boots the old ESP (firmware entry, MAAS PXE handoff) should
# land on the new root as well.
patch_old_esp() {
    local stub=/boot/efi/EFI/ubuntu/grub.cfg
    if [ -f "$stub.pre-moveroot" ]; then
        cp "$stub.pre-moveroot" "$stub"
    else
        cp "$stub" "$stub.pre-moveroot"
    fi
    if grep -q "$OLD_ROOT_UUID" "$stub"; then
        sed -i "s/$OLD_ROOT_UUID/$NEW_ROOT_UUID/g" "$stub"
        log "Pointed the GRUB stub on the old ESP at $NEW_ROOT_UUID (backup in $stub.pre-moveroot)"
    else
        warn "the GRUB stub on the old ESP does not reference $OLD_ROOT_UUID; left unchanged"
    fi
}

final_sync() {
    umount "$MNT/boot/efi"
    # rsync --delete must never see bind mounts of the live system below $MNT.
    [ -z "$(findmnt -Rno TARGET --mountpoint "$MNT" | tail -n +2)" ] ||
        die "unexpected mounts below $MNT; refusing to run rsync --delete"
    log "Final sync of / to $NEW_ROOT"
    rsync_tolerant -aAXH --numeric-ids --one-file-system --delete --info=stats1 \
        --exclude=/etc/fstab --exclude=/etc/fstab.pre-moveroot --exclude=/boot/ \
        --exclude=/var/lib/initramfs-tools/ "${EXCLUDES[@]}" / "$MNT"/
}

write_state() {
    mkdir -p "$MNT$(dirname "$STATE_FILE")"
    {
        printf 'OLD_DISK=%q\n' "$OLD_DISK"
        printf 'NEW_DISK=%q\n' "$NEW_DISK"
        printf 'OLD_ROOT_UUID=%q\n' "$OLD_ROOT_UUID"
        printf 'OLD_ESP_UUID=%q\n' "$OLD_ESP_UUID"
        printf 'OLD_ESP_PARTUUID=%q\n' "$OLD_ESP_PARTUUID"
        printf 'NEW_ROOT_UUID=%q\n' "$NEW_ROOT_UUID"
        printf 'NEW_ESP_UUID=%q\n' "$NEW_ESP_UUID"
    } > "$MNT$STATE_FILE"
    sync
    umount "$MNT"
    rmdir "$MNT" || true
}

main() {
    trap cleanup EXIT
    preflight
    partition_new_disk
    copy_root
    make_bootable
    patch_old_esp
    final_sync
    write_state
    log "The root filesystem is on $NEW_ROOT (UUID $NEW_ROOT_UUID); ready to reboot"
}
"""

_FINISH_SH = r"""
remove_stale_boot_entries() {
    [ -n "${OLD_ESP_PARTUUID:-}" ] || return 0
    local entries num
    entries=$(efibootmgr -v | awk -v partuuid="${OLD_ESP_PARTUUID,,}" '
        index(tolower($0), partuuid) && match($0, /^Boot[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]/) {
            print substr($0, 5, 4)
        }') || entries=""
    for num in $entries; do
        log "Removing EFI boot entry Boot$num, which pointed at the wiped ESP"
        efibootmgr -q -b "$num" -B || warn "could not remove EFI boot entry Boot$num"
    done
}

wipe_old_disk() {
    local old_part old_disk root_disk busy part
    old_part=$(lsblk -lnpo NAME,UUID | awk -v uuid="$OLD_ROOT_UUID" '$2 == uuid { print $1 }')
    if [ -z "$old_part" ]; then
        log "The old root filesystem ($OLD_ROOT_UUID) is gone; nothing to wipe"
        return 0
    fi
    [ "$(wc -l <<<"$old_part")" -eq 1 ] ||
        die "several devices carry UUID $OLD_ROOT_UUID (${old_part//$'\n'/ }); refusing to wipe"
    old_disk=$(parent_disk "$old_part") || die "cannot determine the disk holding $old_part"
    root_disk=$(parent_disk "$(findmnt -no SOURCE /)") || die "cannot determine the disk holding /"
    [ "$old_disk" != "$root_disk" ] ||
        die "the old root $old_part is on the running root disk $root_disk; refusing to wipe"
    busy=$(disk_mountpoints "$old_disk")
    [ -z "$busy" ] || die "$old_disk is still in use (${busy//$'\n'/ }); refusing to wipe"
    busy=$(non_partition_children "$old_disk")
    [ -z "$busy" ] || die "$old_disk has holders (${busy//$'\n'/ }); refusing to wipe"
    if [ "$old_disk" != "$OLD_DISK" ]; then
        log "The old disk is now called $old_disk (it was $OLD_DISK before the reboot)"
    fi

    log "Wiping $old_disk"
    for part in $(partitions_of "$old_disk"); do
        wipefs -aq "$part"
    done
    sgdisk --zap-all "$old_disk" >/dev/null
    wipefs -aq "$old_disk"
    if ! blkdiscard -f "$old_disk"; then
        warn "blkdiscard failed on $old_disk; its signatures are wiped but its blocks were not discarded"
    fi
    udevadm settle || true
    remove_stale_boot_entries
    log "Wiped $old_disk"
}

main() {
    [ "$(id -u)" -eq 0 ] || die "must run as root"
    require_commands awk blkdiscard efibootmgr findmnt lsblk sgdisk udevadm wipefs
    [ -f "$STATE_FILE" ] || die "$STATE_FILE not found; the host did not boot from the moved root"
    # shellcheck disable=SC1090
    . "$STATE_FILE"
    local root_uuid esp_uuid
    root_uuid=$(findmnt -no UUID /)
    [ "$root_uuid" = "$NEW_ROOT_UUID" ] ||
        die "/ is UUID $root_uuid, expected $NEW_ROOT_UUID; leaving $OLD_DISK alone"
    esp_uuid=$(findmnt -no UUID /boot/efi || true)
    [ "$esp_uuid" = "$NEW_ESP_UUID" ] ||
        die "/boot/efi is UUID '$esp_uuid', expected $NEW_ESP_UUID; leaving $OLD_DISK alone"
    log "/ is on $(findmnt -no SOURCE /) and /boot/efi on $(findmnt -no SOURCE /boot/efi)"
    swapon --show || true

    if [ "$WIPE_OLD" = 1 ]; then
        wipe_old_disk
    else
        log "Keeping the old disk as requested"
    fi
    lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT
}
"""


def normalise_device(value: str) -> str:
    """Accept ``sdb`` or ``/dev/...`` and return a ``/dev/...`` path."""
    candidate = value.strip()
    if not candidate.startswith("/dev/"):
        candidate = f"/dev/{candidate}"
    if not DEVICE_RE.fullmatch(candidate):
        raise click.BadParameter(
            f"'{value}' is not a device such as /dev/sdb, sdb or /dev/disk/by-id/..."
        )
    return candidate


def _script(variables: dict[str, str], body: str) -> str:
    header = [f"{key}={shlex.quote(value)}" for key, value in variables.items()]
    return "\n".join(["#!/bin/bash", *header, _COMMON_SH, body, 'main "$@"', ""])


def build_prepare_script(old_dev: str, new_dev: str, *, force: bool = False) -> str:
    return _script(
        {
            "OLD_DEV": old_dev,
            "NEW_DEV": new_dev,
            "FORCE": "1" if force else "0",
            "STATE_FILE": STATE_FILE,
        },
        _PREPARE_SH,
    )


def build_finish_script(*, wipe_old: bool = True) -> str:
    return _script(
        {"WIPE_OLD": "1" if wipe_old else "0", "STATE_FILE": STATE_FILE},
        _FINISH_SH,
    )


def ssh_argv(target: str, command: str, *, connect_timeout: int = 15) -> list[str]:
    return [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "LogLevel=ERROR",
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={connect_timeout}",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=4",
        target,
        command,
    ]


@dataclasses.dataclass(frozen=True)
class Remote:
    """An SSH target plus the side-effect hooks used to talk to it."""

    target: str
    runner: Runner = subprocess.run
    echo: Callable[[str], None] = click.echo
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    retry_for: float = SSH_RETRY_SECONDS

    def run(
        self, command: str, *, input: str | None = None, connect_timeout: int = 15
    ) -> subprocess.CompletedProcess:
        """Run once; exit code 255 means SSH itself failed."""
        return self.runner(
            ssh_argv(self.target, command, connect_timeout=connect_timeout),
            input=input,
            capture_output=True,
            text=True,
            check=False,
        )

    def run_retrying(
        self, command: str, *, input: str | None = None
    ) -> subprocess.CompletedProcess:
        """Run, retrying for a while when the SSH connection fails.

        Only use this for idempotent commands.
        """
        deadline = self.clock() + self.retry_for
        while True:
            result = self.run(command, input=input)
            if result.returncode != 255 or self.clock() >= deadline:
                return result
            self.sleep(SSH_RETRY_INTERVAL)

    def check(
        self, command: str, *, what: str, input: str | None = None
    ) -> subprocess.CompletedProcess:
        result = self.run_retrying(command, input=input)
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            message = (
                f"Failed to {what} on {self.target} (exit code {result.returncode})."
            )
            if detail:
                message += f" {detail}"
            raise click.ClickException(message)
        return result


def _start_unit_once(unit: str, argv: list[str]) -> str:
    """Shell command that starts a transient unit unless it already exists.

    This makes starting safe to retry after a dropped SSH connection.
    """
    probe = shlex.join(["systemctl", "show", "--property=LoadState", "--value", unit])
    return (
        f'if [ "$({probe})" = loaded ]; then echo "{unit} already started"; '
        f"else {shlex.join(argv)}; fi"
    )


@dataclasses.dataclass(frozen=True)
class UnitState:
    load_state: str
    active_state: str
    sub_state: str
    result: str
    exit_status: str

    @property
    def finished(self) -> bool:
        if self.load_state != "loaded":
            return True
        if self.active_state in {"failed", "inactive"}:
            return True
        return self.active_state == "active" and self.sub_state == "exited"

    @property
    def succeeded(self) -> bool:
        return (
            self.load_state == "loaded"
            and self.active_state == "active"
            and self.sub_state == "exited"
            and self.result == "success"
        )

    def describe(self) -> str:
        if self.load_state != "loaded":
            return f"the unit is {self.load_state or 'missing'}"
        return (
            f"state {self.active_state}/{self.sub_state}, result {self.result}, "
            f"exit status {self.exit_status}"
        )


def build_poll_command(unit: str, cursor: str | None) -> str:
    journal = [
        "sudo",
        "journalctl",
        f"_SYSTEMD_UNIT={unit}.service",
        "--output=cat",
        "--no-pager",
        "--quiet",
        "--show-cursor",
    ]
    if cursor:
        journal.append(f"--after-cursor={cursor}")
    show = [
        "sudo",
        "systemctl",
        "show",
        f"{unit}.service",
        "--property=LoadState,ActiveState,SubState,Result,ExecMainStatus",
    ]
    return f"{shlex.join(journal)} || true; echo {POLL_MARKER}; {shlex.join(show)}"


def parse_poll_output(
    stdout: str,
) -> tuple[list[str], str | None, UnitState] | None:
    """Split one poll into new log lines, the journal cursor and unit state."""
    lines = stdout.splitlines()
    try:
        marker = lines.index(POLL_MARKER)
    except ValueError:
        return None
    log_lines: list[str] = []
    cursor: str | None = None
    for line in lines[:marker]:
        if line.startswith(CURSOR_PREFIX):
            cursor = line[len(CURSOR_PREFIX) :].strip() or cursor
        else:
            log_lines.append(line)
    props: dict[str, str] = {}
    for line in lines[marker + 1 :]:
        key, sep, value = line.partition("=")
        if sep:
            props[key.strip()] = value.strip()
    state = UnitState(
        load_state=props.get("LoadState", ""),
        active_state=props.get("ActiveState", ""),
        sub_state=props.get("SubState", ""),
        result=props.get("Result", ""),
        exit_status=props.get("ExecMainStatus", ""),
    )
    return log_lines, cursor, state


def run_remote_step(
    remote: Remote,
    *,
    name: str,
    script: str,
    run_token: str,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    disconnect_timeout: float = DEFAULT_DISCONNECT_TIMEOUT,
) -> None:
    """Run ``script`` as root in a transient unit and stream its output."""
    unit = f"{UNIT_PREFIX}-{name}-{run_token}"
    script_path = f"{REMOTE_SCRIPT_DIR}/{unit}.sh"
    remote.check(
        f"sudo install -d -m 0700 {shlex.quote(REMOTE_SCRIPT_DIR)} && "
        f"sudo tee {shlex.quote(script_path)} >/dev/null",
        input=script,
        what=f"upload the {name} script",
    )
    remote.check(
        _start_unit_once(
            f"{unit}.service",
            [
                "sudo",
                "systemd-run",
                "--quiet",
                f"--unit={unit}",
                "--property=RemainAfterExit=yes",
                f"--description=cephtools moveroot {name}",
                "/bin/bash",
                script_path,
            ],
        ),
        what=f"start the {name} step",
    )
    hint = f"ssh {remote.target} sudo journalctl -u {unit}.service"
    remote.echo(f"Running the {name} step on {remote.target} as {unit}.service.")

    cursor: str | None = None
    last_contact = remote.clock()
    disconnected = False
    draining = False
    try:
        while True:
            result = remote.run(build_poll_command(unit, cursor))
            parsed = (
                parse_poll_output(result.stdout or "")
                if result.returncode != 255
                else None
            )
            if parsed is None:
                if remote.clock() - last_contact > disconnect_timeout:
                    raise click.ClickException(
                        f"Lost contact with {remote.target} for more than "
                        f"{int(disconnect_timeout)}s during the {name} step. "
                        f"It may still be running; check it with: {hint}"
                    )
                if not disconnected:
                    remote.echo(
                        f"Lost the SSH connection to {remote.target}; the {name} "
                        "step keeps running there. Reconnecting..."
                    )
                    disconnected = True
                remote.sleep(poll_interval)
                continue
            if disconnected:
                remote.echo(f"Reconnected to {remote.target}.")
                disconnected = False
            last_contact = remote.clock()
            lines, new_cursor, state = parsed
            cursor = new_cursor or cursor
            for line in lines:
                remote.echo(line)
            if state.finished:
                if not draining:
                    # journald may lag behind the unit; fetch once more.
                    draining = True
                    remote.sleep(1.0)
                    continue
                if state.succeeded:
                    return
                raise click.ClickException(
                    f"The {name} step failed on {remote.target} "
                    f"({state.describe()}). Full log: {hint}"
                )
            remote.sleep(poll_interval)
    except KeyboardInterrupt:
        remote.echo(
            f"Interrupted. The {name} step keeps running; follow it with: {hint}"
        )
        raise


def read_boot_id(remote: Remote, *, retry: bool = False) -> str | None:
    command = "cat /proc/sys/kernel/random/boot_id"
    result = (
        remote.run_retrying(command)
        if retry
        else remote.run(command, connect_timeout=10)
    )
    if result.returncode != 0:
        return None
    return (result.stdout or "").strip() or None


def reboot_host(remote: Remote, run_token: str) -> None:
    # Delay the reboot so the SSH call returns cleanly before sshd goes away.
    unit = f"{UNIT_PREFIX}-reboot-{run_token}"
    remote.check(
        _start_unit_once(
            f"{unit}.timer",
            [
                "sudo",
                "systemd-run",
                "--quiet",
                "--on-active=5",
                f"--unit={unit}",
                "/bin/systemctl",
                "reboot",
            ],
        ),
        what="schedule the reboot",
    )


def wait_for_reboot(
    remote: Remote,
    *,
    previous_boot_id: str,
    timeout: float,
    interval: float = REBOOT_POLL_INTERVAL,
) -> None:
    start = remote.clock()
    next_report = start + 60
    while True:
        boot_id = read_boot_id(remote)
        if boot_id and boot_id != previous_boot_id:
            remote.echo(f"{remote.target} is back up.")
            return
        now = remote.clock()
        if now - start >= timeout:
            raise click.ClickException(
                f"{remote.target} did not come back within {int(timeout)}s of the "
                "reboot. It may still be in firmware or PXE, or it failed to boot "
                "the new root. The old disk has not been wiped; re-run this "
                "command once the host is reachable."
            )
        if now >= next_report:
            remote.echo(
                f"Still waiting for {remote.target} to come back "
                f"({int((now - start) // 60)} min so far)..."
            )
            next_report += 60
        remote.sleep(interval)


@dataclasses.dataclass(frozen=True)
class HostRootState:
    root_uuid: str
    moved_root_uuid: str | None

    @property
    def already_moved(self) -> bool:
        return bool(self.moved_root_uuid) and self.moved_root_uuid == self.root_uuid


def probe_host(remote: Remote) -> HostRootState:
    """Read the UUID of / and any state left by an earlier prepare step."""
    command = (
        f"sudo cat {shlex.quote(STATE_FILE)} 2>/dev/null || true; "
        f"echo {POLL_MARKER}; findmnt -no UUID /"
    )
    result = remote.run_retrying(command)
    lines = (result.stdout or "").splitlines()
    if result.returncode != 0 or POLL_MARKER not in lines:
        detail = (result.stderr or "").strip()
        message = (
            f"Cannot run commands on {remote.target} over SSH "
            f"(exit code {result.returncode})."
        )
        if detail:
            message += f" {detail}"
        raise click.ClickException(message)
    marker = lines.index(POLL_MARKER)
    moved: str | None = None
    for line in lines[:marker]:
        key, sep, value = line.partition("=")
        if sep and key == "NEW_ROOT_UUID":
            parts = shlex.split(value)
            moved = parts[0] if parts else None
    root_uuid = next((line.strip() for line in lines[marker + 1 :] if line.strip()), "")
    return HostRootState(root_uuid=root_uuid, moved_root_uuid=moved)


def _default_run_token() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")


def move_root(
    remote: Remote,
    *,
    old_dev: str,
    new_dev: str,
    force: bool = False,
    wipe_old: bool = True,
    reboot_timeout: float = DEFAULT_REBOOT_TIMEOUT,
    run_token: str | None = None,
) -> None:
    token = run_token or _default_run_token()
    target = remote.target

    state = probe_host(remote)
    if state.already_moved:
        remote.echo(
            f"{target} already runs from the moved root filesystem "
            f"(UUID {state.root_uuid}); skipping the copy and reboot."
        )
    else:
        remote.echo(
            f"Moving the root filesystem of {target} from {old_dev} to {new_dev}."
        )
        run_remote_step(
            remote,
            name="prepare",
            script=build_prepare_script(old_dev, new_dev, force=force),
            run_token=token,
        )
        previous_boot_id = read_boot_id(remote, retry=True)
        if previous_boot_id is None:
            raise click.ClickException(
                f"Could not read the boot id of {target} before rebooting it. The "
                f"new root is ready: reboot the host (ssh {target} sudo reboot) "
                "and re-run this command to check it and finish."
            )
        remote.echo(f"Rebooting {target} into the new root filesystem.")
        reboot_host(remote, token)
        wait_for_reboot(
            remote, previous_boot_id=previous_boot_id, timeout=reboot_timeout
        )

    run_remote_step(
        remote,
        name="finish",
        script=build_finish_script(wipe_old=wipe_old),
        run_token=token,
    )
    if wipe_old:
        remote.echo(f"Done: {target} runs from the new disk and the old disk is wiped.")
    else:
        remote.echo(f"Done: {target} runs from the new disk; the old disk was kept.")
