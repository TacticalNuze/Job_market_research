from celery_app.tasks import scraping_workflow

if __name__ == "__main__":
    scraping_workflow.delay()
