# Resolve the MAAS subnet you want NICs on
data "maas_subnet" "primary" {
  cidr = var.primary_subnet_cidr
}

data "maas_subnet" "external" {
  cidr = var.external_subnet_cidr
}
