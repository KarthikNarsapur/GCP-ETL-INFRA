output "bucket_name" {
  description = "Name of the Cloud Storage bucket."
  value       = google_storage_bucket.bucket.name
}

output "bucket_url" {
  description = "gs:// URL of the Cloud Storage bucket."
  value       = google_storage_bucket.bucket.url
}

output "bucket_self_link" {
  description = "Self-link of the bucket."
  value       = google_storage_bucket.bucket.self_link
}
