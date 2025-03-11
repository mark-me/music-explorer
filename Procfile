web: gunicorn --bind 0.0.0.0:5000 app_explorer:app
worker: celery --app=app_explorer.celery_config worker --pool=solo --loglevel=info
