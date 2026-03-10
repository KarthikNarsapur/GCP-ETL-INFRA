resource "random_id" "instance_suffix" {
  byte_length = 4
}

resource "google_sql_database_instance" "postgres" {
  project             = var.project_id
  name                = "${var.name_prefix}-pg-${random_id.instance_suffix.hex}"
  region              = var.region
  database_version    = var.postgres_version
  deletion_protection = var.deletion_protection

  settings {
    tier              = var.tier
    availability_type = var.availability_type

    disk_size         = var.disk_size_gb
    disk_autoresize   = true
    disk_type         = "PD_SSD"

    # ── Private IP (no public IP) ────────────────────────────────────────────
    ip_configuration {
      ipv4_enabled                                  = false  # No public IP
      private_network                               = var.network_self_link
      enable_private_path_for_google_cloud_services = true
    }

    # ── Backups ──────────────────────────────────────────────────────────────
    backup_configuration {
      enabled            = var.backup_enabled
      start_time         = "03:00"
      binary_log_enabled = false  # Not supported on PostgreSQL

      backup_retention_settings {
        retained_backups = 7
        retention_unit   = "COUNT"
      }
    }

    # ── Maintenance ──────────────────────────────────────────────────────────
    maintenance_window {
      day          = 7  # Sunday
      hour         = 4
      update_track = "stable"
    }

    user_labels = var.labels
  }

  # Ensure the PSA peering exists before creating the instance
  depends_on = [var.private_ip_address_name]
}

# Default database
resource "google_sql_database" "db" {
  project  = var.project_id
  name     = var.db_name
  instance = google_sql_database_instance.postgres.name
}

# Application user
resource "google_sql_user" "app_user" {
  project  = var.project_id
  name     = var.db_user
  password = var.db_password
  instance = google_sql_database_instance.postgres.name
}
