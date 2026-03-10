output "instance_name" {
  description = "Name of the bastion VM."
  value       = google_compute_instance.bastion.name
}

output "internal_ip" {
  description = "Internal (private) IP address of the bastion VM."
  value       = google_compute_instance.bastion.network_interface[0].network_ip
}

output "service_account_email" {
  description = "Service account email attached to the bastion VM."
  value       = google_service_account.bastion.email
}
