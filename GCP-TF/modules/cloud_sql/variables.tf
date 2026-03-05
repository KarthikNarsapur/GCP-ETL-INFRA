variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "GCP region for the Cloud SQL instance."
  type        = string
}

variable "name_prefix" {
  description = "Prefix for resource names (e.g. myproject-dev)."
  type        = string
}

variable "network_self_link" {
  description = "Self-link of the VPC network to attach the instance to."
  type        = string
}

variable "private_ip_address_name" {
  description = "Name of the reserved PSA global address (used as depends_on signal)."
  type        = string
}

variable "postgres_version" {
  description = "PostgreSQL database version string (e.g. POSTGRES_15)."
  type        = string
  default     = "POSTGRES_15"
}

variable "tier" {
  description = "Machine tier for the Cloud SQL instance."
  type        = string
  default     = "db-f1-micro"
}

variable "db_name" {
  description = "Name of the database to create."
  type        = string
}

variable "db_user" {
  description = "Database username."
  type        = string
}

variable "db_password" {
  description = "Database user password."
  type        = string
  sensitive   = true
}

variable "deletion_protection" {
  description = "Enable deletion protection on the instance."
  type        = bool
  default     = false
}

variable "availability_type" {
  description = "ZONAL or REGIONAL (HA)."
  type        = string
  default     = "ZONAL"
}

variable "disk_size_gb" {
  description = "Initial disk size in GB."
  type        = number
  default     = 20
}

variable "backup_enabled" {
  description = "Enable automated backups."
  type        = bool
  default     = true
}

variable "labels" {
  description = "Resource labels."
  type        = map(string)
  default     = {}
}
