data "digitalocean_kubernetes_versions" "current" {
  version_prefix = "1."
}

data "digitalocean_vpc" "default" {
  region = var.region
}

resource "digitalocean_kubernetes_cluster" "hermes" {
  name     = var.cluster_name
  region   = var.region
  version  = data.digitalocean_kubernetes_versions.current.latest_version
  vpc_uuid = data.digitalocean_vpc.default.id

  ha                               = false
  destroy_all_associated_resources = true

  node_pool {
    name       = "${var.cluster_name}-pool"
    size       = var.node_size
    node_count = var.node_count
  }
}