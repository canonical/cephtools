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

Users should ensure that Terraform is aware of the `juju_model` dependency of the charm module.

To deploy this module with its needed dependency, you can run:

```bash
terraform apply -var="model=<MODEL_NAME>"
```

<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
|------|---------|
| terraform | >= 1.5 |
| juju | >= 0.14.0 |

## Providers

| Name | Version |
|------|---------|
| juju | >= 0.14.0 |

## Resources

| Name | Type |
|------|------|
| [juju_application.microceph](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/application) | resource |
| [juju_model.model](https://registry.terraform.io/providers/juju/juju/latest/docs/data-sources/model) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| model | Name of the model to deploy to (must be a machine model) | `string` | n/a | yes |
| app\_name | Name to give the deployed application | `string` | `"microceph"` | no |
| channel | Channel that the charm is deployed from | `string` | `"squid/stable"` | no |
| config | Map of the charm configuration options | `map(string)` | `{}` | no |
| revision | Revision number of the charm | `number` | `null` | no |
| placements | Optional list of Juju placement targets (e.g. machine IDs) | `list(string)` | `[]` | no |
| units | Unit count/scale | `number` | `1` | no |

## Outputs

| Name | Description |
|------|-------------|
| app\_name | The name of the deployed application |
| provides | Map of the integration endpoints provided by the application |
| requires | Map of the integration endpoints required by the application |
<!-- END_TF_DOCS -->
