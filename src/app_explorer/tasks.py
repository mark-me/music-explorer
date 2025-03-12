import yaml

from app_explorer.celery_config import celery_app
from app_explorer.discogs_extractor import Discogs
from app_explorer.task_simulator import TaskSimulator
from utils import ManageDB


@celery_app.task(name="tasks.discogs_etl", bind=True)
def discogs_etl(self):
    with open(r"config/config.yml") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    # Creating a copy of the DB for loading and create a backup
    db = ManageDB(file_db=config["file_db"])
    file_db_load = db.create_load_copy()
    db.create_backup()

    discogs = Discogs(file_secrets="/data/secrets.yml", file_db=file_db_load)
    task = discogs.start_ETL(app_celery=self)
    task.start()
    db.replace_db()
    return {"current": 100, "total": 100, "status": "Task completed!", "result": 42}

@celery_app.task(name="tasks.simulator")
def simulate_etl(self):
    task = TaskSimulator(celery_app=self)
    task.start()
    return {"current": 100, "total": 100, "status": "Task completed!", "result": 42}