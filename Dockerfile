FROM python:3.12.3-slim-bookworm

WORKDIR /app

COPY . /app
RUN python3 -m venv /opt/venv

RUN . /opt/venv/bin/activate && pip install --no-cache-dir --upgrade .

EXPOSE 5000
CMD . /opt/venv/bin/activate && exec honcho start
