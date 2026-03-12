resource "google_container_cluster" "gke" {
  name     = var.cluster_name
  location = var.region
  deletion_protection = false
  network    = var.network
  subnetwork = var.subnetwork

  remove_default_node_pool = true
  node_locations = [
    "asia-south1-a",
    "asia-south1-b"
  ]

  initial_node_count       = 1
  node_config {
    disk_type    = "pd-standard"
    disk_size_gb = 20
  }

  ip_allocation_policy {
    cluster_secondary_range_name  = var.pods_secondary_range
    services_secondary_range_name = var.services_secondary_range
  }
}

resource "google_container_node_pool" "primary_nodes" {
  name       = "primary-node-pool"
  location   = var.region
  cluster    = google_container_cluster.gke.name
  node_count = 1

  node_config {
    machine_type = var.node_machine_type

    disk_type    = "pd-standard"
    disk_size_gb = 20


    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }
}