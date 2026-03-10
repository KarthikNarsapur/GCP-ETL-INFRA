# ─── Enable the API Gateway service ───────────────────────────────────────────
resource "google_project_service" "apigateway" {
  project            = var.project_id
  service            = "apigateway.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "servicecontrol" {
  project            = var.project_id
  service            = "servicecontrol.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "servicemanagement" {
  project            = var.project_id
  service            = "servicemanagement.googleapis.com"
  disable_on_destroy = false
}

# ─── Service Account for API Gateway ──────────────────────────────────────────
resource "google_service_account" "api_gateway" {
  project      = var.project_id
  account_id   = "${var.name_prefix}-apigw-sa"
  display_name = "API Gateway Service Account (${var.name_prefix})"
}

# ─── API ──────────────────────────────────────────────────────────────────────
resource "google_api_gateway_api" "api" {
  provider = google-beta
  project  = var.project_id
  api_id   = "${var.name_prefix}-${var.api_id}"
  labels   = var.labels

  depends_on = [
    google_project_service.apigateway,
    google_project_service.servicecontrol,
    google_project_service.servicemanagement,
  ]
}

# ─── API Config ───────────────────────────────────────────────────────────────
resource "google_api_gateway_api_config" "config" {
  provider      = google-beta
  project       = var.project_id
  api           = google_api_gateway_api.api.api_id
  api_config_id = "${var.name_prefix}-${var.api_id}-config"
  labels        = var.labels

  # openapi_documents {
  #   document {
  #     path     = "openapi.yaml"
  #     contents = base64encode(var.openapi_spec)
  #   }
  # }

  openapi_documents {
  document {
    path     = "openapi.yaml"
    contents = base64encode(file("${path.module}/openapi.yaml"))
  }
}

  gateway_config {
    backend_config {
      google_service_account = google_service_account.api_gateway.email
    }
  }

  lifecycle {
    create_before_destroy = true
  }
}

# ─── Gateway (regional deployment) ───────────────────────────────────────────
resource "google_api_gateway_gateway" "gateway" {
  provider   = google-beta
  project    = var.project_id
  api_config = google_api_gateway_api_config.config.id
  gateway_id = "${var.name_prefix}-${var.api_id}-gw"
  region     = "us-central1"
  labels     = var.labels
}
