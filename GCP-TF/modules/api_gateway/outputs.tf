output "api_id" {
  description = "The API resource ID."
  value       = google_api_gateway_api.api.api_id
}

output "gateway_id" {
  description = "API Gateway gateway resource ID."
  value       = google_api_gateway_gateway.gateway.gateway_id
}

output "gateway_url" {
  description = "Default hostname of the API Gateway (https://...)."
  value       = "https://${google_api_gateway_gateway.gateway.default_hostname}"
}

output "service_account_email" {
  description = "Service account attached to the API Gateway backend."
  value       = google_service_account.api_gateway.email
}
