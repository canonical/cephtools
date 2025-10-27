# 1) Compose N VMs on the LXD VM host (MAAS "VM host")
locals {
  vm_storage_disks = concat(
    [
      {
        size = var.vm_root_disk_size
      }
    ],
    [for _ in range(var.vm_data_disk_count) : { size = var.vm_data_disk_size }]
  )
}

resource "maas_vm_host_machine" "vms" {
  count    = var.vm_count

  vm_host  = var.lxd_vm_host_id
  hostname = format("%s-%02d", var.vm_prefix, count.index + 1)

  cores  = var.vm_cores
  memory = var.vm_memory

  # Disks (GiB). Per-disk pool selection is managed on the VM host in MAAS.
  dynamic "storage_disks" {
    for_each = local.vm_storage_disks
    content {
      size_gigabytes = storage_disks.value.size
    }
  }

  network_interfaces {
    name        = "eth0"
    subnet_cidr = data.maas_subnet.primary.cidr
    fabric      = data.maas_subnet.primary.fabric
  }

  network_interfaces {
    name        = "eth1"
    subnet_cidr = data.maas_subnet.external.cidr
    fabric      = data.maas_subnet.external.fabric
  }
}
output "vm_hostnames"     { value = [for m in maas_vm_host_machine.vms : m.hostname] }
