output "cluster_id" {
  description = "UUID of the DOKS cluster"
  value       = digitalocean_kubernetes_cluster.hermes.id
}

output "cluster_name" {
  description = "Name of the DOKS cluster, used for doctl kubeconfig save"
  value       = digitalocean_kubernetes_cluster.hermes.name
}

output "cluster_endpoint" {
  description = "API server endpoint of the cluster"
  value       = digitalocean_kubernetes_cluster.hermes.endpoint
}

output "cluster_version" {
  description = "Kubernetes version the cluster is running"
  value       = digitalocean_kubernetes_cluster.hermes.version
}

output "node_pool_size" {
  description = "Droplet size slug of the worker nodes"
  value       = digitalocean_kubernetes_cluster.hermes.node_pool[0].size
}