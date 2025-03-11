import yaml
from celery import Celery

with open(r"config/config.yml") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

celery_app = Celery(
    "main", broker=config["celery_broker_url"], backend=config["celery_result_backend"]
)
celery_app.autodiscover_tasks(["app_explorer.tasks"])

# Task routing
celery_app.conf.task_routes = {
    "tasks.discogs_etl": {"queue": "cloud_queue"},
}

celery_app.conf.update(
    result_expires=60,
)


import app_explorer.tasks  # Import tasks module to register tasks

# Check Redis connection
with celery_app.connection() as connection:
    connection.ensure_connection()
    print("Connected to Redis successfully")
