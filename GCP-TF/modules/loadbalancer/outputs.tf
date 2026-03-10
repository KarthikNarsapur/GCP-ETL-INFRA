output "load_balancer_ip" {
  value = google_compute_global_forwarding_rule.forwarding_rule.ip_address
}

output "mig_name" {
  value = google_compute_region_instance_group_manager.mig.name
}