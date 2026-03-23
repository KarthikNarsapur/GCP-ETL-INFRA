import json
import logging
import os
from datetime import datetime
from google.cloud import pubsub_v1
from google.cloud import batch_v1

from config import CONFIG

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# GCP clients
subscriber = pubsub_v1.SubscriberClient()
batch_client = batch_v1.BatchServiceClient()


def _env_value(value):
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return str(value)


def get_env_vars_from_body(body):
    env_vars = []

    for key, value in body.items():
        if key == "row":
            continue
        if key.lower() == "client_id":
            continue

        env_vars.append({
            "name": key.lower(),
            "value": _env_value(value)
        })

    if 'row' in body and isinstance(body['row'], dict):
        for row_key, row_value in body['row'].items():
            if row_key.lower() == "client_id":
                continue

            env_vars.append({
                "name": row_key.lower(),
                "value": _env_value(row_value)
            })

    return env_vars


# def submit_job(message_body, message_id):
#     env_vars = get_env_vars_from_body(message_body)

#     env_vars.append({"name": "workflow_id", "value": CONFIG['WORKFLOW_ID']})
#     env_vars.append({"name": "batch_mode", "value": CONFIG['BATCH_MODE']})
#     env_vars.append({"name": "batch_job_id", "value": CONFIG['BATCH_JOB_ID']})
#     env_vars.append({"name": "client_id", "value": CONFIG['FIXED_CLIENT_ID']})

#     timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
#     job_name = f"invoice-job-{timestamp}-{message_id[-6:]}"

#     job = batch_v1.Job()

#     # Container config
#     runnable = batch_v1.Runnable()
#     runnable.container.image_uri = CONFIG['IMAGE_URI']

#     # Add env variables
#     runnable.environment.variables = {e["name"]: e["value"] for e in env_vars}

#     task = batch_v1.TaskSpec()
#     task.runnables = [runnable]

#     group = batch_v1.TaskGroup()
#     group.task_spec = task
#     group.task_count = 1

#     job.task_groups = [group]

#     parent = f"projects/{CONFIG['PROJECT_ID']}/locations/{CONFIG['REGION']}"

#     response = batch_client.create_job(
#         parent=parent,
#         job_id=job_name,
#         job=job
#     )

#     return response.name

def submit_job(message_body, message_id):
    env_vars = get_env_vars_from_body(message_body)

    env_vars.append({"name": "workflow_id", "value": CONFIG['WORKFLOW_ID']})
    env_vars.append({"name": "batch_mode", "value": CONFIG['BATCH_MODE']})
    env_vars.append({"name": "batch_job_id", "value": CONFIG['BATCH_JOB_ID']})
    env_vars.append({"name": "client_id", "value": CONFIG['FIXED_CLIENT_ID']})

    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    job_name = f"invoice-job-{timestamp}-{message_id[-6:]}"

    job = batch_v1.Job()

    # ✅ Container config
    runnable = batch_v1.Runnable()
    runnable.container.image_uri = CONFIG['IMAGE_URI']

    # ✅ IMPORTANT: add command so busybox doesn't exit instantly
    runnable.container.commands = ["sh", "-c", "echo Hello from Batch && sleep 10"]

    # ✅ Env vars
    runnable.environment.variables = {e["name"]: e["value"] for e in env_vars}

    # Task
    task = batch_v1.TaskSpec()
    task.runnables = [runnable]

    # Optional but recommended
    task.compute_resource.cpu_milli = 2000
    task.compute_resource.memory_mib = 2000

    group = batch_v1.TaskGroup()
    group.task_spec = task
    group.task_count = 1

    job.task_groups = [group]

    # ✅🔥 CRITICAL FIX: set service account
    job.allocation_policy = batch_v1.AllocationPolicy(
        service_account=batch_v1.ServiceAccount(
            email="batch-function-sa@ginthi-entrans.iam.gserviceaccount.com"
            
        )
    )

    parent = f"projects/{CONFIG['PROJECT_ID']}/locations/{CONFIG['REGION']}"

    response = batch_client.create_job(
        parent=parent,
        job_id=job_name,
        job=job
    )

    return response.name

def main(request):
    logger.info("Cloud Function triggered. Pulling Pub/Sub messages...")

    subscription_path = subscriber.subscription_path(
        CONFIG['PROJECT_ID'],
        CONFIG['SUBSCRIPTION_NAME']
    )

    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": 10}
    )

    messages = response.received_messages

    if not messages:
        logger.info("No Pub/Sub messages, triggering scheduled job")

        job_id = submit_job({"name": "scheduler"}, "scheduler")
        logger.info(f"Submitted Job (scheduler): {job_id}")

        return "Triggered scheduled job"

    processed = 0

    for msg in messages:
        try:
            body = json.loads(msg.message.data.decode())

            job_id = submit_job(body, msg.message.message_id)
            logger.info(f"Submitted Job: {job_id}")

            subscriber.acknowledge(
                request={
                    "subscription": subscription_path,
                    "ack_ids": [msg.ack_id]
                }
            )

            processed += 1

        except Exception as e:
            logger.error(f"Error: {str(e)}")

    return f"Processed {processed} messages"