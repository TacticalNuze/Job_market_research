import docker
from celery import Celery, chain, group, shared_task
from docker.types import LogConfig
from dotenv import load_dotenv

from data_extraction.Websites import MarocAnn, Rekrute, bayt, emploi
from database import scraping_upload

# from skillner.skillner_logic import skillner_extract_and_upload

celery_app = Celery("celery_app")
celery_app.config_from_object("celery_app.celeryconfig")


# 🚀 Tâches de scraping


@shared_task(name="rekrute", bind=True, max_retries=3, default_retry_delay=10)
def rekrute_task(self):
    try:
        print("Appel du script rekrute")
        return Rekrute.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script rekrute: {e}")
        raise self.retry(exc=e)


@shared_task(name="bayt", bind=True, max_retries=3, default_retry_delay=10)
def bayt_task(self):
    try:
        print("Appel du script bayt")
        return bayt.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script bayt: {e}")
        raise self.retry(exc=e)


@shared_task(name="marocannonce", bind=True, max_retries=3, default_retry_delay=10)
def marocann_task(self):
    try:
        print("Appel du script maroc annonces")
        return MarocAnn.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script marocann: {e}")
        raise self.retry(exc=e)


@shared_task(name="emploi", bind=True, max_retries=3, default_retry_delay=10)
def emploi_task(self):
    try:
        print("Appel du script emploi")
        return emploi.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script emploi: {e}")
        raise self.retry(exc=e)


# @shared_task(name="extract_skills")
# def extract_skills():
#    try:
#        skillner_extract_and_upload()
#        print("Skillner skill extraction successfull")
#    except Exception as e:
#        print(f"Couldn't extract skills: {e}")


@shared_task(name="scrape_upload")
def scrape_upload():
    try:
        print("Upload des résultats du scraping")
        scraping_upload()
        return "Upload terminé"
    except Exception as e:
        print(f"Exception lors de l'upload : {e}")
        return "Erreur pendant l'upload"


@shared_task(name="spark_cleaning")
def spark_cleaning():
    client = docker.from_env()
    load_dotenv(".docker.env")

    try:
        # Build image from Dockerfile
        client.images.build(
            path="/app/spark_pipeline",
            dockerfile="Dockerfile.spark",
            tag="job_analytics_app-spark_transform",
        )

        # Run the container
        container = client.containers.run(
            image="job_analytics_app-spark_transform",
            name="spark_transform",
            command="spark-submit /opt/transform_job.py",
            volumes={
                "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
            },
            network="job_analytics_app_default",
            environment={
                "MINIO_API": "minio:9000",
                "MINIO_ROOT_USER": "TEST",
                "MINIO_ROOT_PASSWORD": "12345678",
            },
            log_config=LogConfig(
                type=LogConfig.types.JSON, config={"max-size": "10m", "max-file": "3"}
            ),
            detach=True,
            remove=True,
        )
        # Attendre que le job se termine
        exit_status = container.wait()
        if exit_status:
            logs = container.logs(stdout=True, stderr=True).decode("utf-8")

            return logs

    except docker.errors.APIError as e:
        return f"Erreur lors du lancement du Spark job : {str(e)}"


@shared_task(name="scraping_workflow")
def scraping_workflow():
    scraping_tasks = group(emploi_task.s(), rekrute_task.s(), marocann_task.s())
    workflow = chain(scraping_tasks | scrape_upload.si() | spark_cleaning.si())()
    return workflow


if __name__ == "__main__":
    print("You launched the task.py script")
