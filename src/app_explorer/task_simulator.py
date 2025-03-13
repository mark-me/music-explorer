import random
import time


class TaskSimulator:
    def __init__(self, celery_app):
        self.celery = celery_app
        self.dict_status = {
            "collection_value": {"current": 1, "total": 1, "item": "None"},
            "collection_items": {"current": 1, "total": 1, "item": "None"},
            "collection_artists": {"current": 1, "total": 1, "item": "None"},
        }

    def start(self):
        self.celery.update_state(state="PROGRESS")
        self.start_collection_value()
        self.start_collection_items()
        self.celery.update_state(state="SUCCESS")

    def start_collection_value(self):
        i = 0
        total = 1
        self.dict_status.update({"collection_value":{"current": i, "total": total, "item": ""}})
        self.celery.update_state(state="PROGRESS", meta=self.dict_status)
        time.sleep(1)
        i = i + 1
        self.dict_status["collection_value"].update({"current": i, "total": total, "item": ""})
        self.celery.update_state(state="PROGRESS", meta=self.dict_status)

    def start_collection_items(self):
        """Background task that runs a long function with progress reports."""
        verb = ["Starting up", "Booting", "Repairing", "Loading", "Checking"]
        adjective = ["master", "radiant", "silent", "harmonic", "fast"]
        noun = ["solar array", "particle reshaper", "cosmic ray", "orbiter", "bit"]
        message = ""
        total = random.randint(5, 12)
        for i in range(total):
            if not message or random.random() < 0.25:
                message = "{0} {1}...".format(random.choice(verb), random.choice(adjective))
            self.dict_status["collection_items"].update(
                {"current": i, "total": total, "item": message}
            )
            self.celery.update_state(state="PROGRESS", meta=self.dict_status)
            self.start_artist(artist=random.choice(noun))
            time.sleep(1)

    def start_artist(self, artist: str):
        message = artist
        total = random.randint(3, 7)
        for i in range(total):
            self.dict_status["collection_artists"].update(
                {"current": i, "total": total, "item": message}
            )
            self.celery.update_state(state="PROGRESS", meta=self.dict_status)
            time.sleep(1)
