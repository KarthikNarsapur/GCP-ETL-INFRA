variable "project_id" {}
variable "region" {}


variable "cluster_name" {}

variable "network" {}
variable "subnetwork" {}

variable "node_machine_type" {
  default = "e2-micro"
}

variable "pods_secondary_range" {}
variable "services_secondary_range" {}