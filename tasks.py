from celery import Celery
from join_engine import run_join

# Redis acts as both the message broker (task queue) and result backend.
# broker  → where FastAPI pushes tasks
# backend → where Celery stores task status/result so we can poll it
celery_app = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0"
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
)


@celery_app.task(bind=True)
def celery_join_task(self):
    """
    Celery task that runs the out-of-core join.
    `bind=True` gives access to `self` so we can update task state if needed.
    """
    run_join()
    return {"status": "done", "output": "data/result.csv"}
