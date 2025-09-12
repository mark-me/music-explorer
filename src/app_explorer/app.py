import contextlib
import os
from pathlib import Path

import yaml
from flask import Flask, jsonify, redirect, render_template, request, url_for

from app_explorer.analytics import Artists, Collection, Release
from app_explorer.celery_config import celery_app
from app_explorer.discogs_extractor import Discogs
from app_explorer.tasks import discogs_etl, simulate_etl
from log_config import logging

logger = logging.getLogger(__name__)

# Read configuration
with open(r"config/config.yml") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
file_db = config["db_file"]

app = Flask(
    __name__,
    template_folder=f"{os.getcwd()}/src/app_explorer/templates",
    static_folder=f"{os.getcwd()}/src/app_explorer/static",
)

discogs = Discogs(file_secrets="/data/secrets.yml", file_db=file_db)  # Setup for discogs extraction

# ETL task id
id_task_etl = None

def celery_tasks_running() -> str:
    """Get task id of active task

    Returns:
        str: Task id
    """
    id_task_etl = None
    i = celery_app.control.inspect()
    dict_tasks = i.active()
    if not dict_tasks:
        logger.error("Could not inspect Celery: it may be down.")
        return []
    lst_tasks = sum(dict_tasks.values(), [])
    for task in lst_tasks:
        if task["name"] in ["tasks.simulator", "tasks.discogs_etl"]:
            id_task_etl = task["id"]
    return id_task_etl


@app.route("/manifest.json")
def serve_manifest():
    """Serves the web app manifest file.

    This route serves the `manifest.json` file, which provides metadata about the web application.
    """
    return app.send_static_file("manifest.json", mimetype="application/manifest+json")


@app.route("/service-worker.js")
def serve_sw():
    """Serves the service worker file.

    This route serves the `service-worker.js` file, which handles caching and offline functionality
    for the web application.
    """
    return app.send_static_file("service-worker.js")


@app.route("/offline.html")
def offline():
    """Serves the offline page.

    This route serves the `offline.html` file, which is displayed when the web application
    is offline.
    """
    return app.send_static_file("offline.html")


@app.route("/")
@app.route("/home")
def home():
    """Home page.

    This route renders the home page of the application.
    """
    if Path(file_db).exists():
        return render_template("home.html")
    else:
        return redirect(url_for("config_page"))


@app.route("/artists")
def artists():
    """Artists page.

    This route renders the artists page, displaying a selection of random artists and the top 10 artists.
    """
    db_artists = Artists(file_db=file_db)
    lst_random = db_artists.random(qty_sample=5)
    lst_all = db_artists.all_top_10()
    return render_template(
        "artists/artists.html", random_artists=lst_random, all_artists=lst_all, title="Artists"
    )


@app.route("/artists_all")
def artists_all():
    """All artists page.

    This route renders the all artists page, displaying all artists from the database.
    """
    db_artists = Artists(file_db=file_db)
    lst_artists = db_artists.all()
    return render_template("artists/artists_all.html", all_artists=lst_artists, title="Artists")


@app.route("/artists_search")
def artists_search():
    """Searches for artists.

    This route handles artist searches, querying the database based on the provided query parameter.
    If no query is provided, it returns all artists.
    """
    query = request.args.get("query")
    db_artists = Artists(file_db=file_db)
    lst_artists = db_artists.search(query) if query else db_artists.all()
    return render_template("artists/artists_search_results.html", artists=lst_artists)


@app.route("/artist/<int:id_artist>")
def artist(id_artist):
    """Artist details page.

    This route renders the artist details page, displaying information about a specific artist
    and their releases in the user's collection.

    Args:
        id_artist (int): The ID of the artist.
    """
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
    """Collection items page.

    This route renders the main collection items page, displaying a selection of random items
    and the top 10 items from the user's collection.
    """
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
    """All collection items page.

    This route renders the all collection items page, displaying all items from the database.
    """
    collection = Collection(file_db=file_db)
    lst_all = collection.all()
    return render_template(
        "collection_items/collection_items_all.html",
        all_items=lst_all,
        title="Collection items",
    )


@app.route("/collection_items_search")
def collection_items_search():
    """Searches for collection items.

    This route handles searches within the user's collection, querying the database based on
    the provided query parameter. If no query is provided, it returns all collection items.
    """
    query = request.args.get("query")
    db_collection = Collection(file_db=file_db)
    lst_all = db_collection.search(query) if query else db_collection.all()
    return render_template(
        "collection_items/collection_items_search_results.html", all_items=lst_all
    )


@app.route("/collection_item/<int:id_release>")
def collection_item(id_release: int):
    """Collection item details page.

    This route renders the collection item details page for a given release ID.

    Args:
        id_release (int): The ID of the release.
    """
    release = Release(id_release=id_release, file_db=file_db).data()
    return render_template("collection_items/collection_item.html", item=release)


@app.route("/config")
def config_page():
    """Configuration page.

    This route renders the configuration page, allowing the user to manage Discogs credentials
    and initiate the Discogs data extraction process.
    """
    try:
        url = os.environ("MUSIC_EXPLORER_URL", config['url'])
    except TypeError as e:
        url = "http://localhost:5000"
    dict_config = {
        "credentials_ok": discogs.check_user_tokens(),
        "url_discogs": discogs.request_user_access(url_callback=f"{url}/receive-token"),
    }
    return render_template("config.html", config=dict_config)


@app.route("/receive-token", methods=["GET"])
def accept_user_token():
    """Handles the Discogs OAuth token callback.

    This route receives the OAuth verifier from Discogs, saves the user token,
    and redirects to the configuration page.
    """

    discogs.save_user_token(request.args["oauth_verifier"])
    return redirect(url_for("config_page"))


@app.route("/discogs_etl")
def start_ETL():
    """Starts the Discogs ETL task.

    This route triggers the Discogs ETL process using Celery.
    If a task is already running, it returns the existing task ID.
    """
    global id_task_etl
    id_task_etl = celery_tasks_running()
    if not id_task_etl:
        task = discogs_etl.delay()
        id_task_etl = task.id
    return jsonify({"success": True, "task_id": id_task_etl})


@app.route("/task_etl_id")
def get_task_ETL_id():
    """Retrieves the ID of the current ETL task.

    This route returns the ID of the currently running ETL task, or None if no task is active.
    """
    global id_task_etl
    id_task_etl = celery_tasks_running()
    return jsonify({"success": True, "task_id": id_task_etl})


@app.route("/simulate_etl")
def start_simulate_ETL():
    """Starts a simulated ETL task.

    This route initiates a simulated ETL task using Celery and redirects to the configuration page.
    If a task is already running, it reuses the existing task ID.
    """
    global id_task_etl
    id_task_etl = celery_tasks_running()
    if not id_task_etl:
        task = simulate_etl.delay()
        id_task_etl = task.id
    return redirect(url_for("config_page"))


@app.route("/check_task/<task_id>", methods=["GET"])
def check_task(task_id):
    """Checks the status of a Celery task.

    This route retrieves the status and result of a Celery task by its ID.

    Args:
        task_id: The ID of the Celery task.
    """
    task = celery_app.AsyncResult(task_id)
    task_status = {"status": task.state}
    with contextlib.suppress(TypeError):
        task_status |= task.result
    response = jsonify(task_status)
    return response


@app.route("/about")
def about():
    """About page.

    This route renders the about page of the application.
    """
    return render_template("about.html", title="About")


if __name__ == "__main__":
    app.run(debug=True, port=5000)
