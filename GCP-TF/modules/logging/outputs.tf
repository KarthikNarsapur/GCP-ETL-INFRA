output "log_bucket_id" {
  description = "ID of the custom Cloud Logging log bucket."
  value       = google_logging_project_bucket_config.app_log_bucket.id
}

output "gcs_sink_name" {
  description = "Name of the GCS log sink (empty string if disabled)."
  value       = var.log_sink_gcs_enabled ? google_logging_project_sink.gcs_sink[0].name : ""
}

output "bq_sink_name" {
  description = "Name of the BigQuery log sink (empty string if disabled)."
  value       = var.log_sink_bq_enabled ? google_logging_project_sink.bq_sink[0].name : ""
}

output "bq_dataset_id" {
  description = "BigQuery dataset ID for logs (empty string if disabled)."
  value       = var.log_sink_bq_enabled ? google_bigquery_dataset.log_dataset[0].dataset_id : ""
}
