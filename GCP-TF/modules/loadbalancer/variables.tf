variable "project_id" {}
variable "region" {}

variable "network" {}
variable "subnetwork" {}

variable "machine_type" {
  default = "e2-standard-4"
}

variable "instance_count" {
  default = 1
}

variable "template_name" {}
variable "mig_name" {}

variable "databricks_image" {
  description = "Databricks marketplace image"
  default = "projects/databricks-public/global/images/databricks"
}

variable "health_check_name" {}
variable "backend_service_name" {}
variable "url_map_name" {}
variable "http_proxy_name" {}
variable "forwarding_rule_name" {}