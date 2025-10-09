variable "app_name" {
  description = "Name to give the deployed application"
  type        = string
  default     = "microceph"
}

variable "channel" {
  description = "Channel that the charm is deployed from"
  type        = string
  default     = "squid/stable"
}

variable "config" {
  description = "Map of the charm configuration options"
  type        = map(string)
  default     = {}
}

variable "model" {
  description = "Name of the model to deploy to (must be a machine model)"
  type        = string
  nullable    = false
}

variable "revision" {
  description = "Revision number of the charm"
  type        = number
  nullable    = true
  default     = null
}

variable "units" {
  description = "Unit count/scale"
  type        = number
  default     = 1
}
