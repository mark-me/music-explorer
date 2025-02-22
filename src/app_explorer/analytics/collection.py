from collections import defaultdict
import polars as pl

from db_operations import DBStorage


class Collection(DBStorage):
    def __init__(self, file_db, schema="main"):
        super().__init__(file_db, schema)
        self.sql_all = """
            SELECT
                ci.id_release,
                ci.title,
                ci.url_thumbnail,
                ci.url_cover,
                ci.year_released,
                ci.id_master
            FROM collection.main.collection_items ci
        """

    def _dicts_to_dict(self, key_field: str, lst_dicts: list) -> dict:
        dict_results = defaultdict(list)
        for entry in lst_dicts:
            key_value = entry[key_field]
            del entry[key_field]
            dict_results[key_value].append(entry)
        # Convert defaultdict to a regular dict
        dict_results = dict(dict_results)
        return dict_results

    def _add_nested_information(self, lst_items: list) -> list:
        str_release_ids = ", ".join([str(i["id_release"]) for i in lst_items])
        sql_artist = f"""
            SELECT
                ra.id_release,
                ra.id_artist,
                a.name_artist,
            FROM collection.main.release_artists ra
            LEFT JOIN collection.main.artist a
            ON a.id_artist = ra.id_artist
            WHERE ra.id_release IN ({str_release_ids})
        """
        lst_artists = self.read_sql(sql=sql_artist).to_dicts()
        dict_artists = self._dicts_to_dict(key_field="id_release", lst_dicts=lst_artists)

        sql_format = f"""
            SELECT
                id_release,
                name_format
            FROM collection.main.release_formats
            WHERE id_release IN ({str_release_ids})
        """
        lst_formats = self.read_sql(sql=sql_format).to_dicts()
        dict_formats = self._dicts_to_dict(key_field="id_release", lst_dicts=lst_formats)
        # Adding nested information
        for i, item in enumerate(lst_items):
            id_release = item["id_release"]
            lst_items[i].update({"artists": dict_artists[id_release]})
            lst_items[i].update({"formats": dict_formats[id_release]})
        return lst_items

    def all(self) -> list:
        sql = (
            self.sql_all
            + """
        ORDER BY
            LTRIM(
                LTRIM(
                    LTRIM(
                    UPPER(ci.title),
                    '.'),
                    ''''
                    ), '"'
                    )"""
        )
        lst_items = self.read_sql(sql=sql).to_dicts()
        lst_items = self._add_nested_information(lst_items=lst_items)
        return lst_items

    def all_top_10(self) -> list:
        sql = (
            self.sql_all
            + """
        ORDER BY
            REGEXP_REPLACE(UPPER(ci.title), '([.''"])', '')
        LIMIT 10"""
        )
        lst_items = self.read_sql(sql=sql).to_dicts()
        lst_items = self._add_nested_information(lst_items=lst_items)
        return lst_items

    def random(self, qty_sample: int = 20) -> list:
        df = self.read_sql(sql=self.sql_all)
        lst_items = df.sample(n=qty_sample).to_dicts()
        lst_items = self._add_nested_information(lst_items=lst_items)
        return lst_items

    def artist(self, id_artist: str) -> list:
        sql = self.sql_all + f" AND a.id_artist={id_artist} ORDER BY ci.year_released"
        df_releases = self.read_sql(sql=sql)
        lst_release_id = pl.Series(df_releases.select("id_release")).to_list()
        lst_items = df_releases.to_dicts()
        lst_items = self._add_nested_information(lst_items=lst_items)

        # Retrieve tracks
        lst_release_id = [str(i) for i in lst_release_id]
        str_release_ids = ", ".join(lst_release_id)
        sql = f"""
            SELECT
                rt.id_release,
                rt.position,
                rt.title
            FROM release_tracks rt
            WHERE rt.id_release IN ({str_release_ids} )
        """
        lst_tracks = self.read_sql(sql=sql).to_dicts()
        dict_tracks = {}
        for track in lst_tracks:
            id_release = str(track["id_release"])
            if id_release not in dict_tracks:
                dict_tracks.update(
                    {id_release: [{"position": track["position"], "title": track["title"]}]}
                )
            else:
                dict_tracks[id_release].append(
                    {"position": track["position"], "title": track["title"]}
                )

        # Embed tracks in releases
        for i, release in enumerate(lst_items):
            lst_items[i].update({"tracks": dict_tracks[str(release["id_release"])]})

        return lst_items
