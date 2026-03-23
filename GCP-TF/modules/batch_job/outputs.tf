
# Cloud Function URL
output "function_url" {
  value = google_cloudfunctions2_function.function.service_config[0].uri
}

# Pub/Sub Topic
output "pubsub_topic" {
  value = google_pubsub_topic.topic.name
}

# Pub/Sub Subscription
output "pubsub_subscription" {
  value = google_pubsub_subscription.sub.name
}