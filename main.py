import uuid
import logging
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse

from join_engine import run_join
from tasks import celery_join_task, celery_app
from celery.result import AsyncResult

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Scalable Data Processing API")


# ── Approach 1: FastAPI BackgroundTasks ──────────────────────────────────────
#
# How it works:
#   FastAPI queues the task in a background thread (same process).
#   The HTTP response is returned immediately, then the thread runs run_join().
#
# Pros:
#   - Zero extra infrastructure (no Redis, no Celery)
#   - Simple to set up and debug
#   - Good enough for low-traffic / dev environments
#
# Cons:
#   - Runs inside the web server process — a crash in the task can affect the server
#   - No task queue if 100 simultaneous requests = 100 joins running at once
#   - No built-in retry on failure
#   - No way to poll task status without building it yourself
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/trigger-join/background", summary="Approach 1 — BackgroundTasks")
def trigger_join_background(background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    logger.info(f"[Approach 1] Job {job_id} accepted — queuing via BackgroundTasks")
    background_tasks.add_task(run_join)
    return {
        "job_id": job_id,
        "approach": "FastAPI BackgroundTasks",
        "status": "queued",
        "note": "No status polling available for this approach"
    }


# ── Approach 2: Celery + Redis ───────────────────────────────────────────────
#
# How it works:
#   FastAPI pushes a task message to Redis.
#   A separate Celery worker process picks it up and runs run_join()
#   completely independently of the web server.
#
# Pros:
#   - Fully decoupled — web server crash won't kill the running task
#   - Built-in task status tracking (PENDING → STARTED → SUCCESS/FAILURE)
#   - Retry on failure, rate limiting, concurrency control
#   - Horizontally scalable — spin up more workers as needed
#
# Cons:
#   - Requires Redis running as a separate service
#   - More complex setup and deployment
#   - Overkill for simple single-user or dev scenarios
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/trigger-join/celery", summary="Approach 2 — Celery + Redis")
def trigger_join_celery():
    task = celery_join_task.delay()
    logger.info(f"[Approach 2] Job {task.id} accepted — queued via Celery")
    return {
        "job_id": task.id,
        "approach": "Celery + Redis",
        "status": "queued",
        "poll_url": f"/job-status/{task.id}"
    }


@app.get("/job-status/{job_id}", summary="Poll Celery task status")
def get_job_status(job_id: str):
    """
    Returns the current status of a Celery job.
    Possible statuses: PENDING → STARTED → SUCCESS | FAILURE
    """
    try:
        result = AsyncResult(job_id, app=celery_app)
        response = {
            "job_id": job_id,
            "status": result.status,
        }
        if result.status == "SUCCESS":
            response["result"] = result.result
        elif result.status == "FAILURE":
            response["error"] = str(result.result)
        return JSONResponse(content=response)
    except Exception as e:
        logger.error(f"Error polling job {job_id}: {e}")
        return JSONResponse(
            status_code=503,
            content={"job_id": job_id, "status": "error", "error": str(e)}
        )
