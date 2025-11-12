variable "maas_api_url" {
  type = string
}

variable "maas_api_key" {
  type      = string
  sensitive = true
}

variable "lxd_vm_host" {
  type        = string
  description = "MAAS VM host name to compose VMs on."
}

# Defaults tailored to your inputs
variable "vm_count" {
  type    = number
  default = 4
}

variable "vm_prefix" {
  type    = string
  default = "ceph"
}

variable "vm_cores" {
  type    = number
  default = 1
}

variable "vm_memory" {
  type    = number
  default = 8192 # MiB
}

variable "vm_root_disk_size" {
  type    = number
  default = 16
}

variable "vm_data_disk_size" {
  type    = number
  default = 16
}

variable "vm_data_disk_count" {
  type    = number
  default = 1
}

variable "primary_subnet_cidr" {
  type = string
}

variable "external_subnet_cidr" {
  type = string
}

# OS
variable "distro_series" {
  type    = string
  default = "noble" # Ubuntu 24.04
}
