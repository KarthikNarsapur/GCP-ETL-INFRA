output "vpc_name" {
  description = "Name of the VPC network."
  value       = google_compute_network.vpc.name
}

output "vpc_self_link" {
  description = "Self-link of the VPC network."
  value       = google_compute_network.vpc.self_link
}

output "vpc_id" {
  description = "Resource ID of the VPC network."
  value       = google_compute_network.vpc.id
}

output "app_subnet_self_link" {
  description = "Self-link of the application subnet."
  value       = google_compute_subnetwork.app.self_link
}

output "data_subnet_self_link" {
  description = "Self-link of the data subnet."
  value       = google_compute_subnetwork.data.self_link
}

output "psa_address_name" {
  description = "Name of the PSA reserved global address (passed to Cloud SQL module)."
  value       = google_compute_global_address.psa_range.name
}

output "app_subnet_name" {
  value = google_compute_subnetwork.app.name
}


output "pods_range_name" {
  value = "pods"
}

output "services_range_name" {
  value = "services"
}