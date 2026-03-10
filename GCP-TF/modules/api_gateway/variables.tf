variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "GCP region for the API Gateway deployment."
  type        = string
  default = "asia-southeast1"
}

variable "name_prefix" {
  description = "Resource name prefix (e.g. myproject-dev)."
  type        = string
}

variable "api_id" {
  description = "Base identifier for the API (will be prefixed by name_prefix)."
  type        = string
  default     = "main-api"
}

variable "openapi_spec" {
  description = "OpenAPI 2.0 (Swagger) or 3.0 specification string for the API config."
  type        = string
}

variable "labels" {
  description = "Resource labels."
  type        = map(string)
  default     = {}
}
