resource "google_compute_instance_template" "databricks_template" {
  name_prefix  = var.template_name
  machine_type = var.machine_type

    disk {
    boot         = true
    auto_delete  = true
    source_image = var.databricks_image
    disk_size_gb = 100
    disk_type    = "pd-balanced"
  }


  network_interface {
    network    = var.network
    subnetwork = var.subnetwork

    access_config {}
  }

  tags = ["http-server"]
}

resource "google_compute_region_instance_group_manager" "mig" {
  name               = var.mig_name
  region             = var.region
  base_instance_name = "databricks"

  version {
    instance_template = google_compute_instance_template.databricks_template.id
  }

  target_size = var.instance_count
}

resource "google_compute_health_check" "health_check" {
  name = var.health_check_name

  http_health_check {
    port         = 80
    request_path = "/"
  }
}

resource "google_compute_backend_service" "backend" {
  name                  = var.backend_service_name
  protocol              = "HTTP"
  load_balancing_scheme = "EXTERNAL"
  port_name             = "http"

  health_checks = [google_compute_health_check.health_check.id]

  backend {
    group = google_compute_region_instance_group_manager.mig.instance_group
  }
}

resource "google_compute_url_map" "url_map" {
  name            = var.url_map_name
  default_service = google_compute_backend_service.backend.id
}

resource "google_compute_target_http_proxy" "proxy" {
  name    = var.http_proxy_name
  url_map = google_compute_url_map.url_map.id
}

resource "google_compute_global_forwarding_rule" "forwarding_rule" {
  name       = var.forwarding_rule_name
  port_range = "80"
  target     = google_compute_target_http_proxy.proxy.id
}