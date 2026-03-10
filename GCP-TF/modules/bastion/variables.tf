variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "GCP region."
  type        = string
}

variable "zone" {
  description = "GCP zone for the bastion VM."
  type        = string
}

variable "name_prefix" {
  description = "Resource name prefix."
  type        = string
}

variable "subnetwork" {
  description = "Self-link of the subnet to place the bastion VM in."
  type        = string
}

variable "machine_type" {
  description = "Machine type for the bastion VM."
  type        = string
  default     = "c4a-standard-1"
}

variable "image" {
  description = "Boot disk image (family/project format)."
  type        = string
  default     = "projects/debian-cloud/global/images/family/debian-12-arm64"
}

variable "startup_script" {
  description = "Optional startup script to run at first boot."
  type        = string
  default     = ""
}

variable "labels" {
  description = "Resource labels."
  type        = map(string)
  default     = {}
}
