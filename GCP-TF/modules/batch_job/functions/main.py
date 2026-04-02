from config_loader import CONFIG
from google.cloud import batch_v1
import uuid


def main(request):
    client = batch_v1.BatchServiceClient()

    parent = f"projects/{CONFIG['PROJECT_ID']}/locations/{CONFIG['REGION']}"

    job = {
        "task_groups": [
            {
                "task_spec": {
                    "runnables": [
                        {
                            "container": {
                                "image_uri": CONFIG["IMAGE_URI"]
                            }
                        }
                    ],
                    "environment": {
                        "variables": {
                            "workflow_id": CONFIG["WORKFLOW_ID"],
                            "client_id": CONFIG["FIXED_CLIENT_ID"],
                            "ENV_TYPE": "dev"
                        }
                    },
                    "compute_resource": {
                        "cpu_milli": 2000,
                        "memory_mib": 2000
                    }
                }
            }
        ],

        "allocation_policy": {
            "instances": [
                {
                    "policy": {
                        "machine_type": "e2-standard-2"
                    }
                }
            ],
            "network": {
                "network_interfaces": [
                    {
                        "no_external_ip_address": False
                    }
                ]
            }
        },

        "logs_policy": {
            "destination": "CLOUD_LOGGING"
        }
    }

    job_id = f"workflow-{uuid.uuid4().hex[:8]}"

    response = client.create_job(
        parent=parent,
        job=job,
        job_id=job_id
    )

    return {"job_name": response.name}