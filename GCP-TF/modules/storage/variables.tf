variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "bucket_name" {
  description = "Globally unique name for the Cloud Storage bucket."
  type        = string
}

variable "location" {
  description = "Bucket location (e.g. US, EU, ASIA, asia-south1)."
  type        = string
  default     = "ASIA"
}

variable "storage_class" {
  description = "Default storage class: STANDARD | NEARLINE | COLDLINE | ARCHIVE."
  type        = string
  default     = "STANDARD"
}

variable "versioning_enabled" {
  description = "Enable object versioning."
  type        = bool
  default     = true
}

variable "lifecycle_age_days" {
  description = "Transition objects to NEARLINE after N days. Set 0 to disable."
  type        = number
  default     = 90
}

variable "labels" {
  description = "Resource labels."
  type        = map(string)
  default     = {}
}
