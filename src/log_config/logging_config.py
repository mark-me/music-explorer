import logging.config


LOGGING = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "json": {
            "format": "%(asctime)s %(levelname)s %(message)s %(module)s %(funcName)s %(process)d",
            "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
        }
    },
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": "json",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "json",
            "filename": "log.json",
            "maxBytes": 204800,
            "backupCount": 10
        }

    },
    "loggers": {"": {"handlers": ["file"], "level": "DEBUG"}},
}


logging.config.dictConfig(LOGGING)

