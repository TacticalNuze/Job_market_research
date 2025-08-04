import os

import docker
from celery import Celery, chain, shared_task
from docker import errors as dock_errors
from docker.types import LogConfig
from dotenv import load_dotenv

from data_extraction.Websites import MarocAnn, Rekrute, bayt, emploi
from database import scraping_upload

# from skillner.skillner_logic import skillner_extract_and_upload

celery_app = Celery("celery_app")
celery_app.config_from_object("celery_app.celeryconfig")


# 🚀 Tâches de scraping


@shared_task(name="rekrute", bind=True, max_retries=3, default_retry_delay=5)
def rekrute_task(self):
    try:
        print("Appel du script rekrute")
        return Rekrute.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script rekrute: {e}")
        raise self.retry(exc=e)


@shared_task(name="bayt", bind=True, max_retries=3, default_retry_delay=5)
def bayt_task(self):
    try:
        print("Appel du script bayt")
        return bayt.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script bayt: {e}")
        raise self.retry(exc=e)


@shared_task(name="marocannonce", bind=True, max_retries=3, default_retry_delay=5)
def marocann_task(self):
    try:
        print("Appel du script maroc annonces")
        return MarocAnn.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script marocann: {e}")
        raise self.retry(exc=e)


@shared_task(name="emploi", bind=True, max_retries=3, default_retry_delay=5)
def emploi_task(self):
    try:
        print("Appel du script emploi")
        return emploi.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script emploi: {e}")
        raise self.retry(exc=e)


@shared_task(name="scrape_upload")
def scrape_upload():
    try:
        print("Upload des résultats du scraping")
        scraping_upload()
        return "Upload terminé"
    except Exception as e:
        print(f"Exception lors de l'upload : {e}")
        return "Erreur pendant l'upload"


@shared_task(name="skillner_ner")
def skillner_ner():
    client = docker.from_env()
    load_dotenv(".docker.env")
    try:
        print("Fetching the skillner container")
        skillner_image = client.images.get("job_analytics_app-skillner")
    except dock_errors.ImageNotFound as e:
        # Build image from Dockerfile
        print(f"Skillner image couldn't be found, building new one: {e}")
        skillner_image, build_logs = client.images.build(
            path="/app/skillner",
            dockerfile="Dockerfile.skillner",
            tag="job_analytics_app-skillner",
        )
    try:
        # Run the container
        container = client.containers.run(
            image=skillner_image,
            name="skillner_container_temp",
            command="python skillner_logic.py",
            volumes={
                "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"}
            },
            network="job_analytics_app_default",
            environment={
                "MINIO_API": os.getenv("MINIO_API"),
                "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
                "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
            },
            log_config=LogConfig(
                type=LogConfig.types.JSON, config={"max-size": "10m", "max-file": "3"}
            ),
            detach=True,
            remove=True,
        )
    except docker.errors.APIError as e:
        return f"Erreur lors du lancement du conteneur skilner : {str(e)}"

    # Attendre que le job se termine
    exit_status = container.wait()
    if exit_status:
        return "Finished NER task"
    else:
        return "No logs for skillner build"


@shared_task(name="spark_cleaning")
def spark_cleaning():
    client = docker.from_env()
    load_dotenv(".docker.env")
    try:
        print("Fetching the spark container")
        spark_image = client.images.get("job_analytics_app-spark_transform")
    except dock_errors.ImageNotFound as e:
        # Build image from Dockerfile
        print(f"Spark image couldn't be found, building new one: {e}")
        spark_image, build_logs = client.images.build(
            path="/app/spark_pipeline",
            dockerfile="Dockerfile.spark",
            tag="job_analytics_app-spark_transform",
        )
    try:
        # Run the container
        container = client.containers.run(
            image=spark_image,
            name="spark_transform_temp",
            command="spark-submit /opt/transform_job.py",
            volumes={
                "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
            },
            network="job_analytics_app_default",
            environment={
                "MINIO_API": os.getenv("MINIO_API"),
                "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
                "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
            },
            log_config=LogConfig(
                type=LogConfig.types.JSON, config={"max-size": "10m", "max-file": "3"}
            ),
            detach=True,
            remove=True,
        )
    except docker.errors.APIError as e:
        return f"Erreur lors du lancement du Spark job : {str(e)}"

    # Attendre que le job se termine
    exit_status = container.wait()
    if exit_status:
        logs = container.logs(stdout=True, stderr=True).decode("utf-8")
        print(logs)
    else:
        return "No logs for spark build"


@shared_task(name="pipeline_loader")
def pipeline_loader():
    client = docker.from_env()
    load_dotenv(".docker.env")
    try:
        print("Fetching the pipeline_loader container")
        pipeline_loader_image = client.images.get("job_analytics_app-pipeline_loader")
    except dock_errors.ImageNotFound as e:
        # Build image from Dockerfile
        print(f"pipeline_loader image couldn't be found, building new one: {e}")
        pipeline_loader_image, build_logs = client.images.build(
            path="/app/postgres",
            dockerfile="Dockerfile.pipeline",
            tag="job_analytics_app-pipeline_loader",
        )
    try:
        # Run the container
        container = client.containers.run(
            image=pipeline_loader_image,
            name="pipeline_loader_transform_temp",
            command="python load_offers.py",
            volumes={
                "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
            },
            network="job_analytics_app_default",
            environment={
                "MINIO_API": os.getenv("MINIO_API"),
                "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
                "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
                "POSTGRES_USER": os.getenv("POSTGRES_USER"),
                "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
                "POSTGRES_DB": os.getenv("POSTGRES_DB"),
                "DB_HOST": os.getenv("DB_HOST"),
                "DB_PORT": os.getenv("DB_PORT"),
            },
            log_config=LogConfig(
                type=LogConfig.types.JSON, config={"max-size": "10m", "max-file": "3"}
            ),
            detach=True,
            remove=True,
        )
    except docker.errors.APIError as e:
        return f"Erreur lors du lancement du pipeline_loader job : {str(e)}"

    # Attendre que le job se termine
    exit_status = container.wait()
    if exit_status:
        logs = container.logs(stdout=True, stderr=True).decode("utf-8")
        print(logs)
    else:
        return "No logs for pipeline_loader build"


@shared_task(name="scraping_workflow")
def scraping_workflow():
    scraping_tasks = chain(emploi_task.si() | rekrute_task.si() | marocann_task.si())
    workflow = chain(
        scraping_tasks
        | scrape_upload.si()
        | skillner_ner.si()
        | spark_cleaning.si()
        | pipeline_loader.si()
    )()
    return workflow


if __name__ == "__main__":
    print("You launched the task.py script")
