# ─── VPC ──────────────────────────────────────────────────────────────────────
resource "google_compute_network" "vpc" {
  project                 = var.project_id
  name                    = var.vpc_name
  auto_create_subnetworks = false
  description             = "Private VPC managed by Terraform (${var.vpc_name})"
}

# ─── SUBNETS ──────────────────────────────────────────────────────────────────

# Application subnet — API workloads, services
resource "google_compute_subnetwork" "app" {
  project                  = var.project_id
  name                     = "${var.vpc_name}-app"
  region                   = var.region
  network                  = google_compute_network.vpc.self_link
  ip_cidr_range            = var.app_subnet_cidr
  private_ip_google_access = true # Allow VMs to reach Google APIs without NAT

secondary_ip_range {
  range_name    = "pods"
  ip_cidr_range = "10.50.0.0/16"
}

secondary_ip_range {
  range_name    = "services"
  ip_cidr_range = "10.30.0.0/16"
}
}

# Data subnet — Cloud SQL private IP lives in this allocation; bastion VM too
resource "google_compute_subnetwork" "data" {
  project                  = var.project_id
  name                     = "${var.vpc_name}-data"
  region                   = var.region
  network                  = google_compute_network.vpc.self_link
  ip_cidr_range            = var.data_subnet_cidr
  private_ip_google_access = true
}

# ─── PRIVATE SERVICE ACCESS (needed for Cloud SQL private IP) ─────────────────

resource "google_compute_global_address" "psa_range" {
  project       = var.project_id
  name          = "${var.vpc_name}-psa-range"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  address       = var.psa_cidr
  prefix_length = var.psa_prefix_length
  network       = google_compute_network.vpc.id
}

resource "google_service_networking_connection" "psa" {
  network                 = google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.psa_range.name]
  # depends_on = [
  #   time_sleep.wait_for_sql_cleanup
  # ]
}


# ─── CLOUD ROUTER + NAT ───────────────────────────────────────────────────────
# Gives private VMs outbound internet access without a public IP.

resource "google_compute_router" "router" {
  project = var.project_id
  name    = var.nat_router_name
  region  = var.region
  network = google_compute_network.vpc.id
}

resource "google_compute_router_nat" "nat" {
  project                            = var.project_id
  name                               = "${var.nat_router_name}-nat"
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

# ─── FIREWALL RULES ───────────────────────────────────────────────────────────

# Allow SSH from Google's IAP proxy range only
resource "google_compute_firewall" "allow_iap_ssh" {
  project     = var.project_id
  name        = "${var.vpc_name}-allow-iap-ssh"
  network     = google_compute_network.vpc.name
  description = "Allow SSH tunneling via Identity-Aware Proxy from GCP's proxy range."
  direction   = "INGRESS"
  priority    = 1000

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  # Google's IAP TCP forwarding source range
  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["bastion"]
}

# Allow internal VPC communication (all protocols between subnets)
resource "google_compute_firewall" "allow_internal" {
  project     = var.project_id
  name        = "${var.vpc_name}-allow-internal"
  network     = google_compute_network.vpc.name
  description = "Allow all traffic within the VPC."
  direction   = "INGRESS"
  priority    = 1000

  allow {
    protocol = "tcp"
  }
  allow {
    protocol = "udp"
  }
  allow {
    protocol = "icmp"
  }

  source_ranges = [var.app_subnet_cidr, var.data_subnet_cidr]
}

# Deny all other inbound traffic (explicit — lower priority)
resource "google_compute_firewall" "deny_all_ingress" {
  project     = var.project_id
  name        = "${var.vpc_name}-deny-all-ingress"
  network     = google_compute_network.vpc.name
  description = "Deny all ingress traffic not matched by higher-priority rules."
  direction   = "INGRESS"
  priority    = 65534

  deny {
    protocol = "all"
  }

  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_firewall" "allow_http" {
  name    = "${var.vpc_name}-allow-http"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["80"]
  }

  source_ranges = ["10.10.0.0/24", "10.10.1.0/24"]

  target_tags = ["http-server"]
}








# # ─── VPC ──────────────────────────────────────────────────────────────────────
# resource "google_compute_network" "vpc" {
#   project                 = var.project_id
#   name                    = var.vpc_name
#   auto_create_subnetworks = false
#   description             = "Private VPC managed by Terraform (${var.vpc_name})"
# }

# # ─── SUBNETS ──────────────────────────────────────────────────────────────────

# resource "google_compute_subnetwork" "app" {
#   project                  = var.project_id
#   name                     = "${var.vpc_name}-app"
#   region                   = var.region
#   network                  = google_compute_network.vpc.self_link
#   ip_cidr_range            = var.app_subnet_cidr
#   private_ip_google_access = true
# }

# resource "google_compute_subnetwork" "data" {
#   project                  = var.project_id
#   name                     = "${var.vpc_name}-data"
#   region                   = var.region
#   network                  = google_compute_network.vpc.self_link
#   ip_cidr_range            = var.data_subnet_cidr
#   private_ip_google_access = true
# }

# # ─── PRIVATE SERVICE ACCESS ───────────────────────────────────────────────────

# resource "google_compute_global_address" "psa_range" {
#   project       = var.project_id
#   name          = "${var.vpc_name}-psa-range"
#   purpose       = "VPC_PEERING"
#   address_type  = "INTERNAL"
#   address       = var.psa_cidr
#   prefix_length = var.psa_prefix_length
#   network       = google_compute_network.vpc.id
# }

# resource "google_service_networking_connection" "psa" {
#   network                 = google_compute_network.vpc.id
#   service                 = "servicenetworking.googleapis.com"
#   reserved_peering_ranges = [google_compute_global_address.psa_range.name]

#   # depends_on = [
#   #   null_resource.cleanup_psa_connection
#   # ]
# }

# # ─── PSA CLEANUP DURING DESTROY ───────────────────────────────────────────────

# resource "null_resource" "cleanup_psa_connection" {

#   triggers = {
#     network = google_compute_network.vpc.name
#   }

#   depends_on = [
#     google_service_networking_connection.psa
#   ]

#   provisioner "local-exec" {
#     when = destroy

#     command = <<EOT
# echo "Waiting for Cloud SQL backend cleanup..."
# sleep 120
# gcloud services vpc-peerings delete \
#   --network=${self.triggers.network} \
#   --service=servicenetworking.googleapis.com \
#   --quiet || true
# EOT
#   }
# }

# # ─── CLOUD ROUTER + NAT ───────────────────────────────────────────────────────

# resource "google_compute_router" "router" {
#   project = var.project_id
#   name    = var.nat_router_name
#   region  = var.region
#   network = google_compute_network.vpc.id
# }

# resource "google_compute_router_nat" "nat" {
#   project                            = var.project_id
#   name                               = "${var.nat_router_name}-nat"
#   router                             = google_compute_router.router.name
#   region                             = var.region
#   nat_ip_allocate_option             = "AUTO_ONLY"
#   source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

#   log_config {
#     enable = true
#     filter = "ERRORS_ONLY"
#   }
# }

# # ─── FIREWALL RULES ───────────────────────────────────────────────────────────

# resource "google_compute_firewall" "allow_iap_ssh" {
#   project     = var.project_id
#   name        = "${var.vpc_name}-allow-iap-ssh"
#   network     = google_compute_network.vpc.name
#   direction   = "INGRESS"
#   priority    = 1000

#   allow {
#     protocol = "tcp"
#     ports    = ["22"]
#   }

#   source_ranges = ["35.235.240.0/20"]
#   target_tags   = ["bastion"]
# }

# resource "google_compute_firewall" "allow_internal" {
#   project     = var.project_id
#   name        = "${var.vpc_name}-allow-internal"
#   network     = google_compute_network.vpc.name
#   direction   = "INGRESS"
#   priority    = 1000

#   allow { protocol = "tcp" }
#   allow { protocol = "udp" }
#   allow { protocol = "icmp" }

#   source_ranges = [
#     var.app_subnet_cidr,
#     var.data_subnet_cidr
#   ]
# }

# resource "google_compute_firewall" "deny_all_ingress" {
#   project     = var.project_id
#   name        = "${var.vpc_name}-deny-all-ingress"
#   network     = google_compute_network.vpc.name
#   direction   = "INGRESS"
#   priority    = 65534

#   deny {
#     protocol = "all"
#   }

#   source_ranges = ["0.0.0.0/0"]
# }

# resource "google_compute_firewall" "allow_http" {
#   name    = "${var.vpc_name}-allow-http"
#   network = google_compute_network.vpc.name

#   allow {
#     protocol = "tcp"
#     ports    = ["80"]
#   }

#   source_ranges = [
#     "10.10.0.0/24",
#     "10.10.1.0/24"
#   ]

#   target_tags = ["http-server"]
# }