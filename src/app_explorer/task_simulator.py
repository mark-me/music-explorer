import random
import time

from redis import Redis

CHANNEL = "music-explorer"


class TaskSimulator:
    def __init__(self, redis: Redis):
        self.redis = redis

    def publish(self, message: dict):
        self.redis.publish(CHANNEL, message=message)

    def start(self):
        self.publish(
            message={
                "state": "PROGRESS",
                "data": {"step": "start", "current": 0, "total": 0, "item": ""},
            },
        )
        self.start_collection_value()
        self.start_collection_items()
        self.publish(
            message={
                "state": "SUCCESS",
                "data": {"step": "Collection items", "current": 1, "total": 1, "item": "None"},
            },
        )

    def start_collection_value(self):
        i = 0
        total = 1
        step = "Collection value"
        self.publish(
            message={
                "state": "PROGRESS",
                "data": {"step": step, "current": i, "total": total, "item": ""},
            }
        )
        time.sleep(1)
        i = i + 1
        self.publish(
            message={
                "state": "PROGRESS",
                "data": {"step": step, "current": i, "total": total, "item": ""},
            }
        )

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
            self.publish(
                message={
                    "state": "PROGRESS",
                    "data": {
                        "step": "Collection items",
                        "current": i,
                        "total": total,
                        "item": message,
                    },
                }
            )
            self.start_artist(artist=random.choice(noun))
            time.sleep(1)

    def start_artist(self, artist: str):
        message = artist
        total = random.randint(3, 7)
        for i in range(total):
            self.publish(
                message={
                    "state": "PROGRESS",
                    "data": {
                        "step": "Collection artists",
                        "current": i,
                        "total": total,
                        "item": message,
                    },
                }
            )
            time.sleep(1)
