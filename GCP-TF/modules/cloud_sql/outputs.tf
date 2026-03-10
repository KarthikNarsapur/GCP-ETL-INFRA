output "instance_name" {
  description = "Cloud SQL instance name."
  value       = google_sql_database_instance.postgres.name
}

output "private_ip" {
  description = "Private IP address of the Cloud SQL instance."
  value       = google_sql_database_instance.postgres.private_ip_address
}

output "connection_name" {
  description = "Cloud SQL connection name (project:region:instance)."
  value       = google_sql_database_instance.postgres.connection_name
}

output "database_name" {
  description = "Name of the created database."
  value       = google_sql_database.db.name
}
