variable "do_token" {
  description = "DigitalOcean API token used by Terraform to manage resources"
  type        = string
  sensitive   = true
}

variable "region" {
  description = "DigitalOcean region slug where the cluster is created"
  type        = string
  default     = "fra1"
}

variable "cluster_name" {
  description = "Name of the DOKS cluster"
  type        = string
  default     = "hermes"
}

variable "node_size" {
  description = "Droplet size slug for worker nodes in the pool"
  type        = string
  default     = "s-1vcpu-2gb"
}

variable "node_count" {
  description = "Number of worker nodes in the pool"
  type        = number
  default     = 1
}