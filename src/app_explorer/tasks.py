import yaml

from app_explorer.celery_config import celery_app
from app_explorer.discogs_extractor import Discogs

with open(r"config/config.yml") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

discogs = Discogs(file_secrets="/data/secrets.yml", file_db=config["db_file"])


@celery_app.task(name="tasks.discogs_etl", bind=True)
def discogs_etl(self):
    task = discogs.start_ETL(app_celery=self)
    task.start()
    return {"current": 100, "total": 100, "status": "Task completed!", "result": 42}
