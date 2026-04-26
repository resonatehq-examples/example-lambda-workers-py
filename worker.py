# ---------------------------------------------------------------------------
# Resonate worker — long-running Python process that executes the workflow
# ---------------------------------------------------------------------------
#
# The Lambda function (lambda_function.py) is a stateless trigger. The actual
# document processing — OCR extraction, LLM analysis, database storage,
# notifications — runs HERE, in a worker process that has no 15-minute
# execution ceiling.
#
# How it works:
#   1. The worker connects to the Resonate Server in the "worker" group
#   2. It registers `process_document` as a durable function
#   3. When Lambda calls `resonate.begin_rpc("doc/...", "process_document", ...)`
#      against the same Resonate Server, the Server hands the work to this
#      process via the worker group's poll endpoint.
#   4. Each yielded step is checkpointed in the Resonate Server, so a crash
#      (or Lambda timeout, or even a worker restart) only re-runs the failed
#      step, not the whole workflow.
#
# Run it: `python worker.py` (after `resonate serve` is up).

from __future__ import annotations

import time
from threading import Event
from typing import Any

from resonate import Context, Resonate

# ---------------------------------------------------------------------------
# Worker-side Resonate client — joins the "worker" group
# ---------------------------------------------------------------------------

resonate = Resonate.remote(group="worker")


# ---------------------------------------------------------------------------
# Workflow steps — each is checkpointed by Resonate on completion.
# ---------------------------------------------------------------------------

def download_document(_ctx: Context, job: dict[str, Any]) -> int:
    print(f"[download]   job {job['jobId']} — fetching {job['type']} from {job['documentUrl']}")
    time.sleep(0.5)  # simulates download
    page_count = (hash(job["jobId"]) % 20) + 1
    print(f"[download]   job {job['jobId']} — {page_count} pages received")
    return page_count


def extract_text(_ctx: Context, job: dict[str, Any], page_count: int) -> str:
    print(f"[extract]    job {job['jobId']} — extracting text from {page_count} pages (OCR)")
    time.sleep(page_count * 0.05)  # 50ms per page in demo
    text = f"[extracted text from {page_count}-page {job['type']} document]"
    print(f"[extract]    job {job['jobId']} — extraction complete")
    return text


def analyze_document(_ctx: Context, job: dict[str, Any], text: str) -> dict[str, Any]:
    print(f"[analyze]    job {job['jobId']} — sending to LLM for {job['type']} analysis")
    time.sleep(0.8)  # simulates LLM call
    summary = f"{job['type']} document processed. {len(text)} chars analyzed."
    if job["type"] == "invoice":
        data: dict[str, Any] = {"vendor": "Acme Corp", "amount": 4999, "currency": "USD"}
    elif job["type"] == "contract":
        data = {"parties": ["Alice Inc.", "Bob LLC"], "expires": "2027-01-01"}
    else:
        data = {"period": "Q4 2025", "metrics": {"revenue": 1_200_000}}
    print(f"[analyze]    job {job['jobId']} — analysis complete")
    return {"summary": summary, "data": data}


def store_results(_ctx: Context, job: dict[str, Any], summary: str, data: dict[str, Any]) -> str:
    print(f"[store]      job {job['jobId']} — writing results to database")
    time.sleep(0.2)
    stored_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"[store]      job {job['jobId']} — stored at {stored_at}")
    return stored_at


def notify_requester(_ctx: Context, job: dict[str, Any], stored_at: str) -> str:
    print(f"[notify]     job {job['jobId']} — notifying {job['requesterId']} that results are ready")
    time.sleep(0.15)
    notified_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"[notify]     job {job['jobId']} — requester notified")
    return notified_at


# ---------------------------------------------------------------------------
# The durable workflow — registered with Resonate, dispatched from Lambda
# ---------------------------------------------------------------------------

@resonate.register
def process_document(ctx: Context, job: dict[str, Any]):
    """Five-step durable document pipeline.

    Each `yield ctx.run(...)` is checkpointed: if the worker crashes,
    only the unfinished step re-runs on resume.
    """
    page_count = yield ctx.run(download_document, job)
    text = yield ctx.run(extract_text, job, page_count)
    analysis = yield ctx.run(analyze_document, job, text)
    stored_at = yield ctx.run(
        store_results, job, analysis["summary"], analysis["data"]
    )
    notified_at = yield ctx.run(notify_requester, job, stored_at)

    return {
        "jobId": job["jobId"],
        "type": job["type"],
        "pageCount": page_count,
        "summary": analysis["summary"],
        "extractedData": analysis["data"],
        "storedAt": stored_at,
        "notifiedAt": notified_at,
    }


if __name__ == "__main__":
    print("[worker]     starting — registered: process_document")
    print("[worker]     waiting for work from the Resonate Server...")
    resonate.start()
    Event().wait()
