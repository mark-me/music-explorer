import yaml
from flask import Blueprint, render_template, request

from app_loader.discogs_extractor import Discogs

with open(r"config/config.yml") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
file_db = config["db_file"]
discogs = Discogs(file_secrets="config/secrets.yml", file_db=file_db)

bp_authentication = Blueprint("bp_authentication", __name__, template_folder="templates")

@bp_authentication.route("/authenticate")
def authenticate():
    has_user_tokens = discogs.check_user_tokens()
    return render_template(
        "authentication.html",
        has_user_tokens=has_user_tokens,
        title="Collection items",
    )


@bp_authentication.route("/get-user-access", methods=["GET"])
def open_discogs_permissions_page():
    """ Asks user to give app access to Discogs account, with a callback url to handle validation
    """
    callback_url = f"http://localhost:{config['PORT_LOADER']}/discogs/receive-token/"
    result = discogs.request_user_access(callback_url=callback_url)
    return result

@bp_authentication.route("/receive-token", methods=["GET"])
def accept_user_token(): #oauth_token: str, oauth_verifier: str):
    """Callback function to process the user authentication result
    """
    oauth_verifier = request.args.get('oauth_verifier')
    result = discogs.save_user_token(oauth_verifier)
    return result

# @bp_authentication.route("/process_user_data/")
# async def process_user_data(background_tasks: BackgroundTasks):
#     background_tasks.add_task(discogs.process_user_data)
#     return {"message": "Started processing user Discogs data"}
