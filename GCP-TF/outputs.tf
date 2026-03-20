# =============================================================================
# NETWORKING
# =============================================================================

output "vpc_name" {
  description = "Name of the VPC network."
  value       = module.networking.vpc_name
}

output "vpc_self_link" {
  description = "Self-link of the VPC network."
  value       = module.networking.vpc_self_link
}

output "app_subnet_self_link" {
  description = "Self-link of the application private subnet."
  value       = module.networking.app_subnet_self_link
}

output "data_subnet_self_link" {
  description = "Self-link of the data private subnet."
  value       = module.networking.data_subnet_self_link
}

# =============================================================================
# CLOUD SQL
# =============================================================================

output "postgres_instance_name" {
  description = "Cloud SQL instance name."
  value       = module.cloud_sql.instance_name
}

output "postgres_private_ip" {
  description = "Private IP address of the Cloud SQL PostgreSQL instance."
  value       = module.cloud_sql.private_ip
}

output "postgres_connection_name" {
  description = "Cloud SQL connection name (project:region:instance) for use with Cloud SQL Auth Proxy."
  value       = module.cloud_sql.connection_name
}

# =============================================================================
# BASTION VM
# =============================================================================

output "bastion_instance_name" {
  description = "Name of the bastion VM."
  value       = module.bastion.instance_name
}

output "bastion_internal_ip" {
  description = "Internal (private) IP of the bastion VM."
  value       = module.bastion.internal_ip
}

output "bastion_iap_tunnel_command" {
  description = "Ready-to-use gcloud command to open an IAP SSH tunnel to PostgreSQL via the bastion."
  value       = "gcloud compute ssh ${module.bastion.instance_name} --project=${var.project_id} --zone=${var.zone} --tunnel-through-iap -- -L 5432:${module.cloud_sql.private_ip}:5432 -N"
}

# =============================================================================
# API GATEWAY
# =============================================================================

output "api_gateway_url" {
  description = "Default hostname of the deployed API Gateway."
  value       = module.api_gateway.gateway_url
}

output "api_gateway_id" {
  description = "API Gateway resource ID."
  value       = module.api_gateway.gateway_id
}

# =============================================================================
# CLOUD STORAGE
# =============================================================================

output "storage_bucket_name" {
  description = "Name of the Cloud Storage bucket."
  value       = module.storage.bucket_name
}

output "storage_bucket_url" {
  description = "gs:// URL of the Cloud Storage bucket."
  value       = module.storage.bucket_url
}

# =============================================================================
# CLOUD LOGGING
# =============================================================================

output "log_sink_gcs_name" {
  description = "Name of the Cloud Logging sink sending logs to GCS (empty if disabled)."
  value       = module.logging.gcs_sink_name
}

output "log_sink_bq_name" {
  description = "Name of the Cloud Logging sink sending logs to BigQuery (empty if disabled)."
  value       = module.logging.bq_sink_name
}



output "batch_job_pubsub_topic" {
  value = module.batch_job.pubsub_topic
}

output "batch_job_function_url" {
  value = module.batch_job.function_url
}