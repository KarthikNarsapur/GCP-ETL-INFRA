
# Enable Required APIs

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


# Pub/Sub

resource "google_pubsub_topic" "topic" {
  name   = var.pubsub_topic
  labels = var.labels
}

resource "google_pubsub_subscription" "sub" {
  name  = var.pubsub_subscription
  topic = google_pubsub_topic.topic.id

  ack_deadline_seconds = 60
}


# Service Accounts

resource "google_service_account" "function_sa" {
  account_id   = "batch-function-sa"
  display_name = "Batch Function SA"
}

resource "google_service_account" "scheduler_sa" {
  account_id   = "scheduler-invoker-sa"
  display_name = "Scheduler Invoker SA"
}


# IAM Roles (Function Permissions)

resource "google_project_iam_member" "batch_admin" {
  project = var.project_id
  role    = "roles/batch.jobsEditor"
  member  = "serviceAccount:${google_service_account.function_sa.email}"
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


# Zip Function Code

data "archive_file" "function_zip" {
  type        = "zip"
  source_dir  = "${path.module}/function"
  output_path = "${path.module}/function.zip"
}


# Storage Bucket

resource "google_storage_bucket" "bucket" {
  name          = "${var.project_id}-functions-bucket"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
}

resource "google_storage_bucket_object" "archive" {
  name   = "function.zip"
  bucket = google_storage_bucket.bucket.name
  source = data.archive_file.function_zip.output_path
}


# Cloud Function (Gen2)

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
    google_project_service.services
  ]
}


# Cloud Run IAM (SECURE - NO allUsers)

resource "google_cloud_run_service_iam_member" "invoker" {
  project  = var.project_id
  location = var.region

  service = google_cloudfunctions2_function.function.name

  role   = "roles/run.invoker"
  member = "serviceAccount:${google_service_account.scheduler_sa.email}"
}


# Cloud Scheduler (Authenticated)
resource "google_cloud_scheduler_job" "job" {
  name     = var.scheduler_name
  schedule = "*/5 * * * *"
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
    google_cloud_run_service_iam_member.invoker
  ]
}