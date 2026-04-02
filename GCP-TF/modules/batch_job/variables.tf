variable "project_id" {}
variable "region" {}
variable "vpc_name" {}
variable "function_name" {}
variable "scheduler_name" {}

variable "pubsub_topic" {}
variable "pubsub_subscription" {}

variable "labels" {
  type    = map(string)
  default = {}
}