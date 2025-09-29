terraform {
  required_providers {
    maas = {
      source  = "canonical/maas"
      version = "~> 2.0"
    }
  }
}

provider "maas" {
  api_url     = var.maas_api_url
  api_key     = var.maas_api_key
  api_version = "2.0"
}
