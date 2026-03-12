################################################################################
# Networking
################################################################################
module "networking" {
  source = "./modules/networking"

  project_id         = var.project_id
  region             = var.region
  vpc_name           = "${local.name_prefix}-${var.vpc_name}"
  app_subnet_cidr    = var.app_subnet_cidr
  data_subnet_cidr   = var.data_subnet_cidr
  psa_cidr           = var.psa_cidr
  psa_prefix_length  = var.psa_prefix_length
  nat_router_name    = "${local.name_prefix}-${var.nat_router_name}"
  pods_cidr_range = var.pods_cidr_range
  services_cidr_range = var.services_cidr_range
  labels             = local.common_labels
}

################################################################################
# Cloud SQL (PostgreSQL)
################################################################################
module "cloud_sql" {
  source = "./modules/cloud_sql"

  project_id              = var.project_id
  region                  = var.region
  name_prefix             = local.name_prefix
  network_self_link       = module.networking.vpc_self_link
  private_ip_address_name = module.networking.psa_address_name

  postgres_version           = var.postgres_version
  tier                       = var.postgres_tier
  db_name                    = var.postgres_db_name
  db_user                    = var.postgres_user
  db_password                = var.postgres_password
  deletion_protection        = var.postgres_deletion_protection
  availability_type          = var.postgres_availability_type
  disk_size_gb               = var.postgres_disk_size_gb
  backup_enabled             = var.postgres_backup_enabled
  labels                     = local.common_labels

  depends_on = [module.networking]
}

################################################################################
# Bastion VM (SSH tunnel host)
################################################################################
module "bastion" {
  source = "./modules/bastion"

  project_id     = var.project_id
  region         = var.region
  zone           = var.zone
  name_prefix    = local.name_prefix
  subnetwork     = module.networking.data_subnet_self_link
  machine_type   = var.bastion_machine_type
  image          = var.bastion_image
  startup_script = var.bastion_startup_script
  labels         = local.common_labels

  depends_on = [module.networking]
}

################################################################################
# API Gateway
################################################################################
module "api_gateway" {
  source = "./modules/api_gateway"

  project_id      = var.project_id
  region          = var.region
  name_prefix     = local.name_prefix
  api_id          = var.api_id
  openapi_spec    = var.api_openapi_spec
  labels          = local.common_labels
}

################################################################################
# Cloud Storage
################################################################################
module "storage" {
  source = "./modules/storage"

  project_id          = var.project_id
  bucket_name         = local.storage_bucket_name
  location            = var.storage_location
  storage_class       = var.storage_class
  versioning_enabled  = var.storage_versioning_enabled
  lifecycle_age_days  = var.storage_lifecycle_age_days
  labels              = local.common_labels
}

################################################################################
# Cloud Logging
################################################################################
module "logging" {
  source = "./modules/logging"

  project_id          = var.project_id
  name_prefix         = local.name_prefix
  region              = var.region
  log_sink_gcs_enabled = var.log_sink_gcs_enabled
  log_sink_bq_enabled  = var.log_sink_bq_enabled
  log_filter          = var.log_filter
  log_retention_days  = var.log_retention_days
  storage_bucket_name = module.storage.bucket_name
  labels              = local.common_labels
}

################################################################################
# Load Balancer
################################################################################

module "loadbalancer" {
  source = "./modules/loadbalancer"

  project_id = var.project_id
  region     = var.region

  network    = module.networking.vpc_id
  subnetwork = module.networking.app_subnet_self_link

  template_name = "databricks-template"
  mig_name      = "databricks-mig"

  instance_count = 2
  machine_type   = "e2-micro"

  # databricks_image = "projects/databricks-public/global/images/databricks"

  health_check_name    = "tf-health-check"
  backend_service_name = "tf-backend-service"
  url_map_name         = "tf-url-map"
  http_proxy_name      = "tf-http-proxy"
  forwarding_rule_name = "tf-forwarding-rule"
}

################################################################################
# GKE Cluster
################################################################################
module "gke" {
  source = "./modules/gke"

  project_id   = var.project_id
  region       = var.region
  cluster_name = var.cluster_name

  network                  = module.networking.vpc_name
  subnetwork               = module.networking.app_subnet_name
  pods_secondary_range     = module.networking.pods_range_name
  services_secondary_range = module.networking.services_range_name
}