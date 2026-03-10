# ─── Service Account for Bastion ──────────────────────────────────────────────
resource "google_service_account" "bastion" {
  project      = var.project_id
  account_id   = "${var.name_prefix}-bastion-sa"
  display_name = "Bastion VM Service Account (${var.name_prefix})"
}

# Allow the bastion SA to connect to Cloud SQL as a client
resource "google_project_iam_member" "bastion_sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.bastion.email}"
}

# Allow the bastion SA to write logs
resource "google_project_iam_member" "bastion_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.bastion.email}"
}

# Allow bastion SA to write metrics
resource "google_project_iam_member" "bastion_metrics_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.bastion.email}"
}

# ─── Bastion VM ───────────────────────────────────────────────────────────────
resource "google_compute_instance" "bastion" {
  project      = var.project_id
  name         = "${var.name_prefix}-bastion"
  machine_type = "c4a-standard-1"
  # machine_type = "e2-standard-4"          # Use e2-standard-4 or higher for production
  # zone         = var.zone
  zone         = "asia-south1-a"
  tags = ["bastion"] # Matches the IAP-SSH firewall target_tags

  labels = var.labels

  boot_disk {
    initialize_params {
      image = var.image
      size  = 20
      type  = "hyperdisk-balanced"
    }
  }

  network_interface {
    subnetwork = var.subnetwork
    # No access_config block → no public (ephemeral) IP assigned
  }

  service_account {
    email  = google_service_account.bastion.email
    scopes = ["cloud-platform"]
  }

  # Shielded VM options — protects against rootkits and boot-level malware
  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  metadata = {
    enable-oslogin         = "TRUE"  # Use Cloud IAP OS Login
    block-project-ssh-keys = "TRUE"  # Disallow project-level SSH keys
    startup-script         = var.startup_script
  }
}


# resource "google_project_iam_member" "bastion_compute_viewer" {
#   project = var.project_id
#   role    = "roles/compute.viewer"
#   member  = "serviceAccount:${google_service_account.bastion.email}"
# }

resource "google_project_iam_member" "bastion_oslogin" {
  project = var.project_id
  role    = "roles/compute.osLogin"
  member  = "serviceAccount:${google_service_account.bastion.email}"
}

resource "google_project_iam_member" "bastion_instance_admin" {
  project = var.project_id
  role    = "roles/compute.instanceAdmin.v1"
  member  = "serviceAccount:${google_service_account.bastion.email}"
}