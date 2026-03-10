resource "google_storage_bucket" "bucket" {
  project                     = var.project_id
  name                        = var.bucket_name
  location                    = var.location
  storage_class               = var.storage_class
  uniform_bucket_level_access = true   # Best practice: use IAM, not ACLs
  labels                      = var.labels
  force_destroy = true

  versioning {
    enabled = var.versioning_enabled
  }

  # Soft-delete policy — keep deleted objects recoverable for 7 days
  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  # Lifecycle: transition older objects to NEARLINE to save cost
  dynamic "lifecycle_rule" {
    for_each = var.lifecycle_age_days > 0 ? [1] : []
    content {
      condition {
        age = var.lifecycle_age_days
      }
      action {
        type          = "SetStorageClass"
        storage_class = "NEARLINE"
      }
    }
  }

  # Lifecycle: delete non-current versions after 30 days
  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 30
      with_state                  = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }
}
