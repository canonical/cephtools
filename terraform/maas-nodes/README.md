# MAAS + LXD VM host

## Quick start

```bash
export TF_VAR_maas_api_url="http://10.241.1.35:5240/MAAS"
export TF_VAR_maas_api_key="<your MAAS API key>"

# one-time: get the VM host ID for "local-lxd"
maas <profile> vm-hosts read | jq -r '.[]|[.id,.name]'

export TF_VAR_lxd_vm_host_id="<id of local-lxd>"
export TF_VAR_primary_subnet_cidr="<primary CIDR, e.g. 10.241.1.0/24>"
export TF_VAR_external_subnet_cidr="<external CIDR, e.g. 10.250.0.0/24>"

terraform init
terraform apply
```

Defaults create 6 VMs `ceph-01..ceph-06` with 4 vCPU, 8 GiB RAM, an 8 GiB root
disk and one 16 GiB data disk on subnet `10.173.203.0/24`

## Terragrunt usage

Export variables or pass them on the command line exactly as you would with Terraform:


```bash
terragrunt apply \
  -var 'maas_api_url=http://10.241.1.35:5240/MAAS' \
  -var 'maas_api_key=<your MAAS API key>' \
  -var 'lxd_vm_host_id=<id of local-lxd>' \
  -var 'vm_data_disk_size=64' \
  -var 'vm_data_disk_count=2' \
  -var 'primary_subnet_cidr=10.241.1.0/24' \
  -var 'external_subnet_cidr=10.250.0.0/24'
```
