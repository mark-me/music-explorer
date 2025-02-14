import yaml

from flask import Flask, render_template, send_from_directory

from analytics import Artists, Collection
from log_config import logging

app = Flask(__name__)

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
    lst_items = db_artists.all().to_dicts()
    return render_template("artists.html", artists=lst_items, title="Artists")

@app.route("/collection_items")
def collection_items():
    """Collection"""
    collection = Collection(file_db=file_db)
    lst_items = collection.random(qty_sample=5).to_dicts()
    return render_template("collection_items.html", collection_items=lst_items, title="Collection items")


@app.route("/about")
def about():
    """About page"""
    return render_template("about.html", title="About")


if __name__ == "__main__":
    app.run(debug=True, port=88)
