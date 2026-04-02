# DATA

data "google_project" "project" {}

resource "google_service_account_iam_member" "allow_batch_sa" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${data.google_project.project.number}-compute@developer.gserviceaccount.com"

  role   = "roles/iam.serviceAccountUser"
  member = "serviceAccount:${google_service_account.function_sa.email}"
}

# ENABLE APIs
resource "google_project_service" "services" {
  for_each = toset([
    "cloudfunctions.googleapis.com",
    "pubsub.googleapis.com",
    "batch.googleapis.com",
    "cloudscheduler.googleapis.com",
    "artifactregistry.googleapis.com"
  ])

  project = var.project_id
  service = each.key
}


# PUBSUB
resource "google_pubsub_topic" "topic" {
  name   = var.pubsub_topic
  labels = var.labels
}

resource "google_pubsub_subscription" "sub" {
  name  = var.pubsub_subscription
  topic = google_pubsub_topic.topic.id

  ack_deadline_seconds = 60
}


# SERVICE ACCOUNTS
resource "google_service_account" "function_sa" {
  account_id = "${var.function_name}-sa"
  display_name = "Batch Function SA"
}

resource "google_service_account" "scheduler_sa" {
  account_id   = "${var.function_name}-scheduler-sa"
  display_name = "Scheduler Invoker SA"
}


# IAM - FUNCTION PERMISSIONS
resource "google_project_iam_member" "batch_admin" {
  project = var.project_id
  role    = "roles/batch.jobsEditor"
  member  = "serviceAccount:${google_service_account.function_sa.email}"
}

resource "google_project_iam_member" "batch_agent_reporter" {
  project = var.project_id
  role    = "roles/batch.agentReporter"
  member  = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}

resource "google_project_iam_member" "pubsub_subscriber" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.function_sa.email}"
}

resource "google_project_iam_member" "logs_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.function_sa.email}"
}


# ALLOW FUNCTION TO ACT AS COMPUTE SA
resource "google_service_account_iam_member" "allow_act_as_compute_sa" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${data.google_project.project.number}-compute@developer.gserviceaccount.com"

  role   = "roles/iam.serviceAccountUser"
  member = "serviceAccount:${google_service_account.function_sa.email}"
}


# ZIP FUNCTION CODE
data "archive_file" "function_zip" {
  type        = "zip"
  source_dir  = "${path.module}/functions"
  output_path = "${path.module}/functions.zip"
}

# STORAGE
resource "google_storage_bucket" "bucket" {
  name = "${var.project_id}-${var.function_name}-bucket"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
}

# Force redeploy every time
resource "google_storage_bucket_object" "archive" {
  name   = "function-${timestamp()}.zip"
  bucket = google_storage_bucket.bucket.name
  source = data.archive_file.function_zip.output_path
}


# CLOUD FUNCTION (GEN2)
resource "google_cloudfunctions2_function" "function" {
  name     = var.function_name
  location = var.region

  build_config {
    runtime     = "python311"
    entry_point = "main"

    source {
      storage_source {
        bucket = google_storage_bucket.bucket.name
        object = google_storage_bucket_object.archive.name
      }
    }
  }

  service_config {
    available_memory = "512M"
    timeout_seconds  = 300

    environment_variables = {
      ENV_TYPE = "dev"
    }

    service_account_email = google_service_account.function_sa.email
  }

depends_on = [
  google_project_service.services,
  google_project_iam_member.batch_admin,
  google_project_iam_member.batch_agent_reporter,
  google_project_iam_member.pubsub_subscriber,
  google_project_iam_member.logs_writer,
  google_service_account_iam_member.allow_act_as_compute_sa
]
}


# CLOUD RUN IAM (SECURE INVOKE)
resource "google_cloud_run_service_iam_member" "invoker" {
  project  = var.project_id
  location = var.region

  service = google_cloudfunctions2_function.function.name

  role   = "roles/run.invoker"
  member = "serviceAccount:${google_service_account.scheduler_sa.email}"
}


resource "google_project_iam_member" "batch_editor" {
  project = var.project_id
  role    = "roles/batch.jobsEditor"
  member  = "serviceAccount:${google_service_account.function_sa.email}"
}

# CLOUD SCHEDULER (AUTHENTICATED)
resource "google_cloud_scheduler_job" "job" {
  name     = var.scheduler_name
  schedule = "* * * * *"
  region   = var.region

  http_target {
    uri         = google_cloudfunctions2_function.function.service_config[0].uri
    http_method = "GET"

    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
      audience              = google_cloudfunctions2_function.function.service_config[0].uri
    }
  }

  depends_on = [
    google_cloud_run_service_iam_member.invoker,
    google_cloudfunctions2_function.function
  ]
}

resource "google_compute_firewall" "allow_batch_https_egress" {
  name    = "allow-${var.function_name}-egress"
  network = var.vpc_name

  direction = "EGRESS"
  priority  = 1000

  allow {
    protocol = "tcp"
    ports    = ["443"]
  }

  allow {
    protocol = "tcp"
    ports    = ["53"]
  }

  allow {
    protocol = "udp"
    ports    = ["53"]
  }

  destination_ranges = ["0.0.0.0/0"]

  target_service_accounts = [
    "${data.google_project.project.number}-compute@developer.gserviceaccount.com"
  ]
}