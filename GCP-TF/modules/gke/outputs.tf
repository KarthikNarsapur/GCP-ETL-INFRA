output "cluster_name" {
  description = "Name of the GKE cluster."
  value       = google_container_cluster.gke.name
}

output "cluster_endpoint" {
  description = "Endpoint of the GKE cluster."
  value       = google_container_cluster.gke.endpoint
}