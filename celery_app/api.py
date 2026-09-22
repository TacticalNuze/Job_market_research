from celery.result import AsyncResult
from fastapi import FastAPI

from celery_app.tasks import celery_app, scraping_workflow

app = FastAPI()


@app.get("/")
def root():
    return {"message": "API de Job Analytics prête 🚀"}


@app.post("/run-scraping")
def run_scraping():
    task = scraping_workflow.delay()
    return {"message": "Scraping lancé", "task_id": task.id}


@app.get("/task-status/{task_id}")
def get_status(task_id: str):
    result = AsyncResult(task_id, app=celery_app)
    return {
        "task_id": task_id,
        "status": result.state,
        "result": str(result.result),
    }
