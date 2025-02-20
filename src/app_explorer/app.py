import yaml
from flask import Flask, render_template

from analytics import Artists, Collection
from app_explorer.route_auth.auth import bp_authentication
from log_config import logging

logger = logging.getLogger(__name__)

app = Flask(__name__) #, template_folder=os.getcwd() + "/src/app_explorer/templates")
EXPLAIN_TEMPLATE_LOADING = True
app.register_blueprint(bp_authentication, url_prefix="/discogs_auth")

with open(r"config/config.yml") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
file_db = config["db_file"]


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
        "artists.html", random_artists=lst_random, all_artists=lst_all, title="Artists"
    )


@app.route("/artists_all")
def artists_all():
    """Artists"""
    db_artists = Artists(file_db=file_db)
    lst_all = db_artists.all()
    return render_template("artists_all.html", all_artists=lst_all, title="Artists")


@app.route("/collection_items")
def collection_items():
    """Collection"""
    collection = Collection(file_db=file_db)
    lst_random = collection.random(qty_sample=5)
    lst_all = collection.all_top_10()
    return render_template(
        "collection_items.html",
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
        "collection_items_all.html",
        all_items=lst_all,
        title="Collection items",
    )


@app.route("/collection_artist/<int:id_artist>")
def collection_artist(id_artist):
    collection = Collection(file_db=file_db)
    artists = Artists(file_db=file_db)
    dict_artist = artists.artist(id_artist=id_artist)
    lst_all = collection.artist(id_artist=id_artist)
    return render_template(
        "collection_items_artist.html",
        all_items=lst_all,
        artist=dict_artist,
        title="Collection items",
    )


@app.route("/about")
def about():
    """About page"""
    return render_template("about.html", title="About")


if __name__ == "__main__":
    app.run(debug=True, port=5000)
