import yaml

from flask import Flask, render_template

from analytics import Artists, Collection

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
    lst_random = db_artists.random(qty_sample=5).to_dicts()
    lst_all = db_artists.all_top_10().to_dicts()
    return render_template("artists.html", random_artists=lst_random , all_artists=lst_all, title="Artists")


@app.route("/collection_items")
def collection_items():
    """Collection"""
    collection = Collection(file_db=file_db)
    lst_random = collection.random(qty_sample=5).to_dicts()
    lst_all = collection.all_top_10().to_dicts()
    return render_template(
        "collection_items.html", random_items=lst_random, all_items=lst_all, title="Collection items"
    )


@app.route("/about")
def about():
    """About page"""
    return render_template("about.html", title="About")


if __name__ == "__main__":
    app.run(debug=True, port=88)
