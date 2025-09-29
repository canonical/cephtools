variable "app_name" {
  description = "Name to give the deployed application"
  type        = string
  default     = "microceph"
}

variable "model_uuid" {
  description = "UUID of the model to deploy to"
  type        = string
  nullable    = false
}

variable "units" {
  description = "Unit count/scale"
  type        = number
  default     = 1
}

variable "charm_microceph_channel" {
  description = "Channel that the microceph charm is deployed from"
  type        = string
  default     = "squid/stable"
}

variable "charm_microceph_revision" {
  description = "Revision number of the charm"
  type        = number
  nullable    = true
  default     = null
}

variable "base" {
  description = "Base to deploy the microceph charm with"
  type        = string
  default     = "ubuntu@24.04"
}

variable "snap_channel" {
  description = "Snap channel for the microceph workload"
  type        = string
  default     = "latest/stable"
}
