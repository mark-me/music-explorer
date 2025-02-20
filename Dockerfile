FROM python:3.12.3-slim-bookworm

WORKDIR /app

COPY . /app
RUN python3 -m venv /opt/venv
RUN . /opt/venv/bin/activate && pip install .


EXPOSE 5000
ENV FLASK_APP=src/app_explorer/app.py
CMD . /opt/venv/bin/activate && exec gunicorn --bind 0.0.0.0:5000 app_explorer:app
