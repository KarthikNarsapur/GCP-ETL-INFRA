locals {
  # Short name prefix shared across all resources: e.g. "myproject-prod"
  name_prefix = "${var.project_id}-${var.environment}"

  # Common labels applied to every resource that supports them
  common_labels = {
    project     = var.project_id
    environment = var.environment
    managed_by  = "terraform"
  }

  # Derive a bucket name if the user did not provide one
  storage_bucket_name = var.storage_bucket_name != "" ? var.storage_bucket_name : "${local.name_prefix}-files"

  # Fully-qualified VPC self-link (used by modules that need it)
  vpc_self_link = module.networking.vpc_self_link
}
