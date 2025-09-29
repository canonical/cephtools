# Terraform module for MicroCeph

This is a Terraform module facilitating the deployment of the `microceph` charm,
using the [Terraform Juju provider](https://github.com/juju/terraform-provider-juju/).
For more information,
refer to the provider [documentation](https://registry.terraform.io/providers/juju/juju/latest/docs).

> [!IMPORTANT]
> This module requires a Juju machine model to be available.
> Refer to the [usage section](#usage) below for more details.

> [!NOTE] This module temporarily lives here until the microceph charm provides its own.

## Usage

Provide the model UUID when applying the module:

```bash
terraform apply -var="model_uuid=<MODEL_UUID>"
```

<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
|------|---------|
| terraform | >= 1.5 |
| juju | >= 1.0.0 |

## Providers

| Name | Version |
|------|---------|
| juju | >= 1.0.0 |

## Resources

| Name | Type |
|------|------|
| [juju_application.microceph](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/application) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| model\_uuid | UUID of the model to deploy to | `string` | n/a | yes |
| app\_name | Name to give the deployed application | `string` | `"microceph"` | no |
| units | Unit count/scale | `number` | `1` | no |
| charm\_microceph\_channel | Channel that the microceph charm is deployed from | `string` | `"squid/stable"` | no |
| charm\_microceph\_revision | Revision number of the charm | `number` | `null` | no |
| base | Base to deploy the microceph charm with | `string` | `"ubuntu@24.04"` | no |
| snap\_channel | Snap channel for the microceph workload | `string` | `"latest/stable"` | no |

## Outputs

| Name | Description |
|------|-------------|
| app\_name | The name of the deployed application |
| provides | Map of the integration endpoints provided by the application |
| requires | Map of the integration endpoints required by the application |
<!-- END_TF_DOCS -->
