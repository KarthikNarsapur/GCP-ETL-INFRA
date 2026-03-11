# ─── Custom Log Bucket (with configurable retention) ──────────────────────────
resource "google_logging_project_bucket_config" "app_log_bucket" {
  project        = var.project_id
  location       = var.region
  retention_days = var.log_retention_days
  bucket_id      = "${var.name_prefix}-logs"

  description = "Custom log bucket for ${var.name_prefix} — ${var.log_retention_days}d retention"

  # lifecycle {
  #   prevent_destroy = true
  # }
}

# ─── Log Sink → Cloud Storage ─────────────────────────────────────────────────
resource "google_logging_project_sink" "gcs_sink" {
  count = var.log_sink_gcs_enabled ? 1 : 0

  project                = var.project_id
  name                   = "${var.name_prefix}-sink-gcs"
  destination            = "storage.googleapis.com/${var.storage_bucket_name}"
  filter                 = var.log_filter
  unique_writer_identity = true

  exclusions {
    name        = "exclude-health-checks"
    description = "Exclude noisy load-balancer health-check log entries"
    filter      = "resource.type=http_load_balancer AND jsonPayload.statusDetails=response_sent_by_backend"
  }
}

# Grant the sink's writer SA permission to write objects to the bucket
# resource "google_storage_bucket_iam_member" "gcs_sink_writer" {
#   count = var.log_sink_gcs_enabled ? 1 : 0

#   bucket = var.storage_bucket_name
#   role   = "roles/storage.objectCreator"
#   member = google_logging_project_sink.gcs_sink[0].writer_identity
# }

resource "google_storage_bucket_iam_member" "gcs_sink_writer" {
  count = var.log_sink_gcs_enabled ? 1 : 0

  depends_on = [
    google_logging_project_sink.gcs_sink
  ]

  bucket = var.storage_bucket_name
  role   = "roles/storage.objectCreator"
  member = google_logging_project_sink.gcs_sink[0].writer_identity
}


# ─── Log Sink → BigQuery ──────────────────────────────────────────────────────
resource "google_bigquery_dataset" "log_dataset" {
  count = var.log_sink_bq_enabled ? 1 : 0

  project                    = var.project_id
  dataset_id                 = replace("${var.name_prefix}_logs", "-", "_")
  location                   = var.region
  description                = "Aggregated logs for ${var.name_prefix}"
  delete_contents_on_destroy = true

  labels = var.labels
}

resource "google_logging_project_sink" "bq_sink" {
  count = var.log_sink_bq_enabled ? 1 : 0
    depends_on = [
    google_bigquery_dataset.log_dataset
  ]

  project                = var.project_id
  name                   = "${var.name_prefix}-sink-bq"
  destination            = "bigquery.googleapis.com/projects/${var.project_id}/datasets/${google_bigquery_dataset.log_dataset[0].dataset_id}"
  filter                 = var.log_filter
  unique_writer_identity = true

  bigquery_options {
    use_partitioned_tables = true  # Cost-effective querying of large log volumes
  }
}

# Grant the sink's writer SA permission to write to BigQuery
resource "google_bigquery_dataset_iam_member" "bq_sink_writer" {
  count = var.log_sink_bq_enabled ? 1 : 0
    depends_on = [
    google_logging_project_sink.bq_sink
  ]

  project    = var.project_id
  dataset_id = google_bigquery_dataset.log_dataset[0].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = google_logging_project_sink.bq_sink[0].writer_identity
}
