# ---------------------------------------------------------------------------
# AWS Lambda handler — API Gateway -> Lambda -> Resonate workflow
# ---------------------------------------------------------------------------
#
# This Lambda function is a STATELESS TRIGGER. It:
#   1. Receives a POST /process-document request via API Gateway
#   2. Dispatches a durable workflow to the Resonate worker (non-blocking)
#   3. Returns 202 Accepted immediately
#
# The actual document processing runs on a Resonate worker process
# (long-running Python server, see worker.py) that can outlast Lambda's
# 15-minute hard timeout.
#
# Why not run the workflow in Lambda itself?
#
#   Lambda timeout: 15 minutes max.
#   Document processing with large files + LLM analysis: potentially hours.
#   Human-in-the-loop review: potentially days.
#
#   Lambda can't hold state between timeouts. Resonate can.
#
# The role split:
#   Lambda is a thin trigger — its only job is to call `resonate.rpc(...)`
#   and return 202. The Resonate Server holds the durable promise. The worker
#   process (worker.py) executes each step and checkpoints progress.

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

from resonate.resonate import Resonate

# ---------------------------------------------------------------------------
# Lambda-side Resonate client — created once per container cold start
# ---------------------------------------------------------------------------
# Module-level: keeps the Resonate connection warm across invocations within
# the same Lambda container. Configured to talk to a remote Resonate Server.

RESONATE_URL = os.environ.get("RESONATE_URL", "http://localhost:8001")

resonate = Resonate(url=RESONATE_URL, group="gateway")

JSON_HEADERS = {"Content-Type": "application/json"}


def _response(status_code: int, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": JSON_HEADERS,
        "body": json.dumps(payload),
    }


# ---------------------------------------------------------------------------
# POST /process-document — dispatch a durable document processing job
# ---------------------------------------------------------------------------

def _handle_process_document(event: dict[str, Any]) -> dict[str, Any]:
    body_raw = event.get("body") or "{}"
    try:
        body = json.loads(body_raw) if isinstance(body_raw, str) else body_raw
    except json.JSONDecodeError:
        return _response(400, {"error": "invalid JSON body"})

    job_id = body.get("jobId")
    document_url = body.get("documentUrl")
    requester_id = body.get("requesterId")
    job_type = body.get("type", "report")

    if not job_id or not document_url or not requester_id:
        return _response(
            400,
            {"error": "jobId, documentUrl, and requesterId are required"},
        )

    job = {
        "jobId": job_id,
        "documentUrl": document_url,
        "requesterId": requester_id,
        "type": job_type,
    }

    print(f"[lambda]     dispatching job {job_id} ({job_type}) to worker group")

    # Non-blocking: hands the work to the Resonate Server. The worker picks
    # it up via the worker group's poll endpoint. Lambda exits without
    # waiting for the workflow to finish.
    resonate.options(target="worker").rpc(
        f"doc/{job_id}",
        "process_document",
        job,
    )

    return _response(
        202,
        {
            "status": "accepted",
            "jobId": job_id,
            "statusUrl": f"/status/{job_id}",
            "message": "Processing in background. Poll statusUrl for results.",
        },
    )


# ---------------------------------------------------------------------------
# GET /status/:jobId — poll for workflow result
# ---------------------------------------------------------------------------

async def _status_async(job_id: str) -> dict[str, Any]:
    handle = await resonate.get(f"doc/{job_id}")
    if not handle.done():
        return _response(200, {"status": "processing", "jobId": job_id})

    result = await handle.result()
    return _response(200, {"status": "done", "jobId": job_id, "result": result})


def _handle_status(event: dict[str, Any]) -> dict[str, Any]:
    path_params = event.get("pathParameters") or {}
    job_id = path_params.get("jobId")

    if not job_id:
        return _response(400, {"error": "jobId is required"})

    try:
        return asyncio.run(_status_async(job_id))
    except Exception as err:  # promise not found, etc.
        print(f"[lambda]     status lookup failed for {job_id}: {err}")
        return _response(404, {"status": "not_found", "jobId": job_id})


# ---------------------------------------------------------------------------
# Lambda entry point — routes API Gateway events
# ---------------------------------------------------------------------------

def lambda_handler(event: dict[str, Any], _context: Any = None) -> dict[str, Any]:
    method = event.get("httpMethod", "")
    path = event.get("path", "")

    print(f"[lambda]     {method} {path}")

    if method == "POST" and path == "/process-document":
        return _handle_process_document(event)

    if method == "GET" and path.startswith("/status/"):
        return _handle_status(event)

    return _response(404, {"error": "Not found"})


# ---------------------------------------------------------------------------
# Local invocation helper — simulate an API Gateway event without AWS
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    import uuid

    job_id = sys.argv[1] if len(sys.argv) > 1 else f"job-{uuid.uuid4().hex[:8]}"

    sample_event = {
        "httpMethod": "POST",
        "path": "/process-document",
        "body": json.dumps(
            {
                "jobId": job_id,
                "documentUrl": "s3://my-bucket/contract.pdf",
                "requesterId": "user-alice",
                "type": "contract",
            }
        ),
        "pathParameters": None,
    }

    print(json.dumps(lambda_handler(sample_event), indent=2))
    print(f"\nPoll: python -c \"import lambda_function as l, json; "
          f"print(json.dumps(l.lambda_handler({{'httpMethod': 'GET', "
          f"'path': '/status/{job_id}', 'pathParameters': {{'jobId': '{job_id}'}}}}), "
          f"indent=2))\"")
