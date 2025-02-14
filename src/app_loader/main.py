import yaml

from discogs_extractor import Discogs


def main():
    with open(r"config/config.yml") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    file_db = config["db_file"]
    discogs = Discogs(file_secrets="config/secrets.yml", file_db=file_db)
    discogs.start_ETL()


if __name__ == "__main__":
    main()
