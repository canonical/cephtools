resource "juju_application" "microceph" {
  name       = var.app_name
  model_uuid = var.model_uuid
  units      = var.units
  trust      = true

  charm {
    name     = "microceph"
    channel  = var.charm_microceph_channel
    revision = var.charm_microceph_revision
    base     = var.base
  }

  config = {
    snap-channel = var.snap_channel
  }
}
