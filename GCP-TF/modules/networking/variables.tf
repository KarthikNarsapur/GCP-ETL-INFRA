variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "GCP region."
  type        = string
}

variable "vpc_name" {
  description = "Name of the VPC to create."
  type        = string
}

variable "app_subnet_cidr" {
  description = "CIDR for the application subnet."
  type        = string
}

variable "data_subnet_cidr" {
  description = "CIDR for the data subnet."
  type        = string
}

variable "psa_cidr" {
  description = "Starting address for the Private Service Access reserved range."
  type        = string
}

variable "psa_prefix_length" {
  description = "Prefix length for the PSA range."
  type        = number
}

variable "nat_router_name" {
  description = "Name of the Cloud Router used by Cloud NAT."
  type        = string
}

variable "labels" {
  description = "Labels to apply to resources that support them."
  type        = map(string)
  default     = {}
}

variable "pods_cidr_range" {
  description = "CIDR for the pods secondary range."
  type        = string
}

variable "services_cidr_range" {
  description = "CIDR for the services secondary range."
  type        = string
}