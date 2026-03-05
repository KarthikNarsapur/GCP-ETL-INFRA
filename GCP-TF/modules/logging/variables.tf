variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "GCP region for the custom log bucket."
  type        = string
}

variable "name_prefix" {
  description = "Resource name prefix."
  type        = string
}

variable "log_sink_gcs_enabled" {
  description = "When true, create a log sink that exports to Cloud Storage."
  type        = bool
  default     = true
}

variable "log_sink_bq_enabled" {
  description = "When true, create a log sink that exports to BigQuery."
  type        = bool
  default     = true
}

variable "log_filter" {
  description = "Advanced log filter for all sinks (empty string = all logs)."
  type        = string
  default     = ""
}

variable "log_retention_days" {
  description = "Retention period in days for the custom Cloud Logging bucket."
  type        = number
  default     = 30
}

variable "storage_bucket_name" {
  description = "Name of the existing Cloud Storage bucket to use as a GCS sink destination."
  type        = string
}

variable "labels" {
  description = "Resource labels for BigQuery dataset."
  type        = map(string)
  default     = {}
}
