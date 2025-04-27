# Music Explorer: Rediscover Your Music Collection

Do you ever feel like you're listening to the same albums over and over? With this app, you can breathe new life into your Discogs collection by getting random recommendations for artists or records you own.

🎵 Explore Your Collection Differently – Let the app surprise you with albums you may have forgotten.

🔀 Randomized Suggestions – Get inspired by unexpected picks from your collection.

📀 Perfect for Large Collections – No more decision fatigue—just hit refresh and start listening!

Start rediscovering your music today!

## Start guide

### Preconditions

* Discogs account you use to register your collection information
* `data` directory in your root directory which should be writeable en readable to the user running the script
* Docker and Docker Compose V2 installed

### How to start using this

#### Setting up the environment

* Clone this repository
* I assumed you have python and [pip](https://python.land/virtual-environments/installing-packages-with-pip) installed.
* Create Python venv
  * ```python -m venv .venv```
* Install requirements:
  * ```pip install -e .```
* Make a /data directory, that is accessible to your user
  * ```sudo mkdir /data```
  * ```sudo chown -R username:usergroup /data```
* I assumed you have docker and docker compose v2 installed
* Start a redis docker container
  * ```sudo docker compose -f docker-compose-redis.yml up -d```

#### Running the project

There are two scenario's to run this project:

1. Run the whole project:
   * Run ```hocho start```
   * Navigate to [http://localhost:5000](http://localhost:5000)

2. Just run the celery worker to be able to debug the project:
    * Start the celery worker, make sure you keep this command running in a terminal. This allows the script to fire op load jobs:
       * ```celery --app=app_explorer.celery_config worker --loglevel=info```
    * Start a debug session with the file ```src/app_explorer/app.py``` from your editor of choice.

## Project documentation

The project's code documentation can be found [here](https://mark-me.github.io/music-explorer/).
