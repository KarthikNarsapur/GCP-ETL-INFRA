# =============================================================================
# GLOBAL
# =============================================================================

variable "project_id" {
  description = "GCP project ID where all resources will be created."
  type        = string
}

variable "region" {
  description = "GCP region for regional resources (e.g. us-central1, asia-south1)."
  type        = string
  default     = "asia-south1"
}

variable "zone" {
  description = "GCP zone for zonal resources such as the bastion VM."
  type        = string
  default     = "asia-south1-a"
}

variable "environment" {
  description = "Deployment environment label (dev | staging | prod). Used in resource names and labels."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

# =============================================================================
# NETWORKING
# =============================================================================

variable "vpc_name" {
  description = "Base name for the VPC network."
  type        = string
  default     = "main"
}

variable "app_subnet_cidr" {
  description = "CIDR range for the application / API private subnet."
  type        = string
  default     = "10.10.0.0/24"
}

variable "data_subnet_cidr" {
  description = "CIDR range for the data (Cloud SQL / bastion) private subnet."
  type        = string
  default     = "10.10.1.0/24"
}

variable "psa_cidr" {
  description = "CIDR prefix for the private service access (Cloud SQL private IP) reserved range. /16 is recommended."
  type        = string
  default     = "10.20.0.0"
}

variable "psa_prefix_length" {
  description = "Prefix length for the private service access reserved range."
  type        = number
  default     = 16
}

variable "nat_router_name" {
  description = "Name for the Cloud Router used by Cloud NAT."
  type        = string
  default     = "nat-router"
}

# =============================================================================
# CLOUD SQL (PostgreSQL)
# =============================================================================

variable "postgres_tier" {
  description = "Cloud SQL machine tier (e.g. db-f1-micro, db-n1-standard-2)."
  type        = string
  default     = "db-f1-micro"
}

variable "postgres_version" {
  description = "PostgreSQL major version to use."
  type        = string
  default     = "POSTGRES_15"
}

variable "postgres_db_name" {
  description = "Name of the default database to create inside the instance."
  type        = string
  default     = "appdb"
}

variable "postgres_user" {
  description = "PostgreSQL username."
  type        = string
  default     = "appuser"
}

variable "postgres_password" {
  description = "PostgreSQL password. Mark sensitive in terraform.tfvars."
  type        = string
  sensitive   = true
}

variable "postgres_deletion_protection" {
  description = "Set to true to prevent accidental deletion of the Cloud SQL instance."
  type        = bool
  default     = false
}

variable "postgres_availability_type" {
  description = "REGIONAL (HA) or ZONAL for Cloud SQL."
  type        = string
  default     = "ZONAL"
}

variable "postgres_disk_size_gb" {
  description = "Initial disk size in GB for the Cloud SQL instance."
  type        = number
  default     = 20
}

variable "postgres_backup_enabled" {
  description = "Enable automated backups for Cloud SQL."
  type        = bool
  default     = true
}

# =============================================================================
# BASTION VM
# =============================================================================

variable "bastion_machine_type" {
  description = "Machine type for the bastion / SSH-tunnel VM."
  type        = string
  default     = "e2-micro"
}

variable "bastion_image" {
  description = "OS image for the bastion VM (family/project format)."
  type        = string
  default     = "debian-cloud/debian-12"
}

variable "bastion_startup_script" {
  description = "Optional startup script content to run on the bastion VM at first boot."
  type        = string
  default     = ""
}

# =============================================================================
# API GATEWAY
# =============================================================================

variable "api_id" {
  description = "Unique API identifier used in API Gateway resource names (lowercase, hyphens allowed)."
  type        = string
  default     = "main-api"
}

variable "api_openapi_spec" {
  description = "Inline OpenAPI 2.0 (Swagger) or OpenAPI 3.0 specification YAML/JSON string for the API Gateway config."
  type        = string
  # A minimal placeholder — replace with your real spec.
  default = <<-YAML
    swagger: "2.0"
    info:
      title: "Main API"
      description: "API Gateway placeholder spec"
      version: "1.0.0"
    host: "placeholder.example.com"
    schemes:
      - "https"
    paths:
      /health:
        get:
          summary: "Health check"
          operationId: "healthCheck"
          responses:
            "200":
              description: "OK"
  YAML
}

# =============================================================================
# CLOUD STORAGE
# =============================================================================

variable "storage_bucket_name" {
  description = "Globally unique name for the Cloud Storage bucket. Leave empty to auto-generate from project_id + environment."
  type        = string
  default     = ""
}

variable "storage_location" {
  description = "Location for the Cloud Storage bucket (multi-region: US, EU, ASIA; or regional)."
  type        = string
  default     = "ASIA"
}

variable "storage_class" {
  description = "Default storage class: STANDARD | NEARLINE | COLDLINE | ARCHIVE."
  type        = string
  default     = "STANDARD"
}

variable "storage_versioning_enabled" {
  description = "Enable object versioning on the bucket."
  type        = bool
  default     = true
}

variable "storage_lifecycle_age_days" {
  description = "Move objects to NEARLINE after this many days (0 to disable lifecycle rule)."
  type        = number
  default     = 90
}

# =============================================================================
# CLOUD LOGGING
# =============================================================================

variable "log_sink_gcs_enabled" {
  description = "Export logs to a Cloud Storage bucket."
  type        = bool
  default     = true
}

variable "log_sink_bq_enabled" {
  description = "Export logs to a BigQuery dataset."
  type        = bool
  default     = true
}

variable "log_filter" {
  description = "Advanced log filter applied to all sinks (empty string = all logs)."
  type        = string
  default     = ""
}

variable "log_retention_days" {
  description = "Retention period in days for the custom Cloud Logging log bucket."
  type        = number
  default     = 30
}


#==============================================================================
# LOAD BALANCER + MIG
#==============================================================================
# LOAD BALANCER + MIG
#==============================================================================
# variable "project_id" {}
# variable "region" {}
variable "databricks_image" {
  description = "Databricks marketplace image"
  default = "projects/databricks-public/global/images/databricks"
}