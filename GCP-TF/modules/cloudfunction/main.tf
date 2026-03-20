terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0, < 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# -----------------------------------
# Storage bucket (for function code)
# -----------------------------------
resource "google_storage_bucket" "bucket" {
  name          = "${var.project_id}-function-bucket"
  location      = var.region
  force_destroy = true
  uniform_bucket_level_access = true
}

# -----------------------------------
# Zip function code
# -----------------------------------
data "archive_file" "function_zip" {
  type        = "zip"
  output_path = "/tmp/function.zip"

  source {
    content = <<EOF
count = 0  # Global variable (per instance)

def main(request):
    global count
    count += 1
    return f"Function has been called {count} times 🚀"
EOF
    filename = "main.py"
  }
}

# Upload zip to bucket
resource "google_storage_bucket_object" "object" {
  name   = "function-${data.archive_file.function_zip.output_md5}.zip"  
  bucket = google_storage_bucket.bucket.name
  source = data.archive_file.function_zip.output_path
}

# -----------------------------------
# Service Account
# -----------------------------------
resource "google_service_account" "sa" {
  account_id   = "cloud-function-sa"
  display_name = "Cloud Function SA"
}

# -----------------------------------
# Cloud Function (Gen2)
# -----------------------------------
resource "google_cloudfunctions2_function" "function" {
  name     = "my-function"
  location = var.region

  build_config {
    runtime     = "python311"
    entry_point = "main"

    source {
      storage_source {
        bucket = google_storage_bucket.bucket.name
        object = google_storage_bucket_object.object.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "256M"
    timeout_seconds       = 60
    service_account_email = google_service_account.sa.email
  }
}

# -----------------------------------
# Allow public access (optional)
# -----------------------------------
resource "google_cloud_run_service_iam_member" "invoker" {
  location = google_cloudfunctions2_function.function.location
  service  = google_cloudfunctions2_function.function.name
  role     = "roles/run.invoker"
  member = "user:riyazuddin.b@ginthi.ai"
}



#==============================================================================
# Scheduler Trigger
#==============================================================================
resource "google_cloud_scheduler_job" "function_trigger" {
  name        = "trigger-my-function"
  project     = var.project_id
  region      = var.region
  description = "Run every 1 minute"

  schedule  = "* * * * *"
  time_zone = "Asia/Kolkata"

  http_target {
    uri         = google_cloudfunctions2_function.function.service_config[0].uri
    http_method = "GET"

    oidc_token {
      service_account_email = google_service_account.sa.email
    }
  }
}

resource "google_cloud_run_service_iam_member" "scheduler_invoker" {
  location = google_cloudfunctions2_function.function.location
  service  = google_cloudfunctions2_function.function.name
  role     = "roles/run.invoker"

  member = "serviceAccount:${google_service_account.sa.email}"
}