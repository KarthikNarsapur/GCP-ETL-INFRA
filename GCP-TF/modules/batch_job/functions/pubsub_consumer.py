import base64
import json

def main(event, context):
    payload = json.loads(
        base64.b64decode(event["data"]).decode("utf-8")
    )

    from batch_inference.workflow.recon_workflow_server_batch import (
        run_dynamic_workflow_batch
    )

    result = run_dynamic_workflow_batch(**payload)

    return result