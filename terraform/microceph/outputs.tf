output "app_name" {
  value       = juju_application.microceph.name
  description = "The name of the deployed application"
}

output "requires" {
  value = {
    traefik_route_rgw = "traefik-route-rgw"
    identity_service  = "identity-service"
    receive_ca_cert   = "receive-ca-cert"
  }
  description = "Map of the integration endpoints required by the application"
}

output "provides" {
  value = {
    ceph      = "ceph"
    radosgw   = "radosgw"
    mds       = "mds"
    cos_agent = "cos-agent"
  }
  description = "Map of the integration endpoints provided by the application"
}
