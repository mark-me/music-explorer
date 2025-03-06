import os
import time

import yaml
from celery import Celery
from flask import Flask, redirect, render_template, request, url_for

from app_explorer.analytics import Artists, Collection, Release
from app_explorer.discogs_extractor import Discogs
from log_config import logging

logger = logging.getLogger(__name__)


# Setup celery and redis broker
def make_celery(app: Flask) -> Celery:
    celery = Celery(
        app.import_name,
        backend=app.config["CELERY_RESULT_BACKEND"],
        broker=app.config["CELERY_BROKER_URL"],
    )
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery


app = Flask(
    __name__,
    template_folder=os.getcwd() + "/src/app_explorer/templates",
    static_folder=os.getcwd() + "/src/app_explorer/static",
)
app.config.update(
    CELERY_BROKER_URL="redis://redis:6379", CELERY_RESULT_BACKEND="redis://redis:6379"
)

celery = make_celery(app)

status_task_ETL = 0

# Read configuration
with open(r"/data/config.yml") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
file_db = config["db_file"]

# Setup for discogs extraction
discogs = Discogs(file_secrets="/data/secrets.yml", file_db=file_db)


@app.route("/manifest.json")
def serve_manifest():
    return app.send_static_file("manifest.json", mimetype="application/manifest+json")


@app.route("/service-worker.js")
def serve_sw():
    return app.send_static_file("service-worker.js")


@app.route("/offline.html")
def offline():
    return app.send_static_file("offline.html")


@app.route("/")
@app.route("/home")
def home():
    """Home page"""
    return render_template("home.html")


@app.route("/artists")
def artists():
    """Artists"""
    db_artists = Artists(file_db=file_db)
    lst_random = db_artists.random(qty_sample=5)
    lst_all = db_artists.all_top_10()
    return render_template(
        "artists/artists.html", random_artists=lst_random, all_artists=lst_all, title="Artists"
    )


@app.route("/artists_all")
def artists_all():
    """Artists"""
    db_artists = Artists(file_db=file_db)
    lst_artists = db_artists.all()
    return render_template("artists/artists_all.html", all_artists=lst_artists, title="Artists")


@app.route("/artists_search")
def artists_search():
    query = request.args.get("query")
    db_artists = Artists(file_db=file_db)
    if query:
        lst_artists = db_artists.search(query)
    else:
        lst_artists = db_artists.all()
    return render_template("artists/artists_search_results.html", artists=lst_artists)


@app.route("/artist/<int:id_artist>")
def artist(id_artist):
    db_collection = Collection(file_db=file_db)
    artists = Artists(file_db=file_db)
    dict_artist = artists.artist(id_artist=id_artist)
    lst_all = db_collection.artist(id_artist=id_artist)
    return render_template(
        "artists/artist.html",
        all_items=lst_all,
        artist=dict_artist,
        title="Collection items",
    )


@app.route("/collection_items")
def collection_items():
    """Collection"""
    collection = Collection(file_db=file_db)
    lst_random = collection.random(qty_sample=5)
    lst_all = collection.all_top_10()
    return render_template(
        "collection_items/collection_items.html",
        random_items=lst_random,
        all_items=lst_all,
        title="Collection items",
    )


@app.route("/collection_items_all")
def collection_items_all():
    """Collection"""
    collection = Collection(file_db=file_db)
    lst_all = collection.all()
    return render_template(
        "collection_items/collection_items_all.html",
        all_items=lst_all,
        title="Collection items",
    )


@app.route("/collection_items_search")
def collection_items_search():
    query = request.args.get("query")
    db_collection = Collection(file_db=file_db)
    if query:
        lst_all = db_collection.search(query)
    else:
        lst_all = db_collection.all()
    return render_template(
        "collection_items/collection_items_search_results.html", all_items=lst_all
    )


@app.route("/collection_item/<int:id_release>")
def collection_item(id_release: int):
    release = Release(id_release=id_release, file_db=file_db).data()
    return render_template("collection_items/collection_item.html", item=release)


@app.route("/config")
def config_page():
    url_callback = f"{config['url']}/receive-token"
    dict_config = {
        "credentials_ok": discogs.check_user_tokens(),
        "url_discogs": discogs.request_user_access(url_callback=url_callback),
        "id_task_etl": status_task_ETL,
    }
    return render_template("config.html", config=dict_config)


@app.route("/receive-token", methods=["GET"])
def accept_user_token():
    """Callback function to process the user authentication result"""
    discogs.save_user_token(request.args["oauth_verifier"])
    return redirect(url_for("config_page"))


@app.route("/start_etl")
def start_ETL():
    global status_task_ETL
    status_task_ETL = task_ETL.delay()


@celery.task(bind=True)
def task_ETL(self):
    """Starting Discogs ETL"""
    i = 0
    while i < 100:
        time.sleep(10)
        logger.info(f"Running background task, iteration {i}")
        i = i + 1
    #discogs.start_ETL()


@app.route("/about")
def about():
    """About page"""
    return render_template("about.html", title="About")


if __name__ == "__main__":
    app.run(debug=True, port=5000)
