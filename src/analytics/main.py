import yaml

from analytics import Artists, Collection


def main():
    with open(r"config/config.yml") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    file_db = config["db_file"]
    collection = Collection(file_db=file_db)
    df_collection = collection.all()
    df_collection


if __name__ == "__main__":
    main()
