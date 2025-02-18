FROM python:3.12.3-slim-bookworm

WORKDIR /app

COPY . /app
RUN pip install .


EXPOSE 5000
ENV FLASK_APP=src/app_explorer/app.py
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "app_explorer:app"]