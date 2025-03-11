import logging
import random
import time

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)
logger.setLevel(logging.INFO)  # Set logging level to INFO
handler = logging.FileHandler("my_log.log")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

class TaskSimulator:
    def __init__(self, celery_app):
        self.celery = celery_app

    def start(self):
        logger.info("Starting steps")
        self.celery.update_state(
                state="PROGRESS", meta={"step": "start", "current": 0, "total": 0, "item": ""}
            )
        self.start_collection_value()
        self.start_collection_items()
        self.celery.update_state(
                state="SUCCESS", meta={"step": "Collection items", "current": 1, "total": 1, "item": "None"}
            )

    def start_collection_value(self):
        i=0
        total=1
        step = "Collection value"
        self.celery.update_state(
                state="PROGRESS", meta={"step": step, "current": i, "total": total, "item": ""}
            )
        logger.info(step)
        time.sleep(1)
        i = i + 1
        self.celery.update_state(
                state="PROGRESS", meta={"step": step, "current": i, "total": total, "item": ""}
            )

    def start_collection_items(self):
        """Background task that runs a long function with progress reports."""
        verb = ["Starting up", "Booting", "Repairing", "Loading", "Checking"]
        adjective = ["master", "radiant", "silent", "harmonic", "fast"]
        noun = ["solar array", "particle reshaper", "cosmic ray", "orbiter", "bit"]
        message = ""
        total = random.randint(5, 12)
        logger.info("Collection items")
        for i in range(total):
            if not message or random.random() < 0.25:
                message = "{0} {1}...".format(
                    random.choice(verb), random.choice(adjective)
                )
                logger.info(f"Collection item {message}")
            self.celery.update_state(
                state="PROGRESS", meta={"step": "Collection items", "current": i, "total": total, "item": message}
            )
            self.start_artist(artist=random.choice(noun))
            time.sleep(1)

    def start_artist(self, artist: str):
        message = artist
        total = random.randint(3, 7)
        for i in range(total):
            self.celery.update_state(
                state="PROGRESS", meta={"step": "Collection artists", "current": i, "total": total, "item": message}
            )
            logger.info(f"Artist {message}")
            time.sleep(1)