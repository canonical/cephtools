# -------------- # Model --------------

data "juju_model" "model" {
  name = var.model
}

# -------------- # Application --------------

resource "juju_application" "microceph" {
  name  = var.app_name
  model = data.juju_model.model.name
  # We always need this variable to be true in order
  # to be able to apply resources limits.
  trust = true
  charm {
    name     = "microceph"
    channel  = var.channel
    revision = var.revision
  }
  units     = length(var.placements) > 0 ? length(var.placements) : var.units
  config    = var.config
  placement = length(var.placements) > 0 ? var.placements : null
}
