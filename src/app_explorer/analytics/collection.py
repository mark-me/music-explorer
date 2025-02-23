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
        sql = (
            self.sql_all
            + f"""
                INNER JOIN collection.main.release_artists ra
                ON ci.id_release = ra.id_release
                WHERE ra.id_artist={id_artist} ORDER BY ci.year_released
            """
        )
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
            FROM collection.main.release_tracks rt
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

    def formats(self) -> list:
        sql = """
            SELECT
                rf.name_format,
                COUNT(*) as qty_collection_items
            FROM collection.main.collection_items ci
            INNER JOIN collection.main.release_formats rf
            ON rf.id_release = ci.id_release
            GROUP BY
                rf.name_format
            ORDER BY
                qty_collection_items DESC
        """
        lst_formats = self.read_sql(sql=sql).to_dicts()
        return lst_formats


    def _add_nested_information(self, lst_items: list) -> list:
        str_release_ids = ", ".join([str(i["id_release"]) for i in lst_items])
        dict_artists = self._artists(str_release_ids=str_release_ids)
        dict_formats = self._formats(str_release_ids=str_release_ids)
        dict_genres = self._genres(str_release_ids=str_release_ids)
        dict_styles = self._styles(str_release_ids=str_release_ids)

        # Adding nested information
        for i, item in enumerate(lst_items):
            id_release = item["id_release"]
            if id_release in dict_artists:
                lst_items[i].update({"artists": dict_artists[id_release]})
            if id_release in dict_formats:
                lst_items[i].update({"formats": dict_formats[id_release]})
            if id_release in dict_genres:
                lst_items[i].update({"genres": dict_genres[id_release]})
            if id_release in dict_styles:
                lst_items[i].update({"styles": dict_styles[id_release]})
        return lst_items

    def _artists(self, str_release_ids: str) -> dict:
        sql = f"""
            SELECT
                ra.id_release,
                ra.id_artist,
                a.name_artist,
            FROM collection.main.release_artists ra
            LEFT JOIN collection.main.artist a
            ON a.id_artist = ra.id_artist
            WHERE ra.id_release IN ({str_release_ids})
        """
        lst_results = self.read_sql(sql=sql).to_dicts()
        dict_results = self._dicts_to_dict(key_field="id_release", lst_dicts=lst_results)
        return dict_results

    def _formats(self, str_release_ids: str) -> dict:
        sql = f"""
            SELECT
                id_release,
                name_format
            FROM collection.main.release_formats
            WHERE id_release IN ({str_release_ids})
        """
        lst_results = self.read_sql(sql=sql).to_dicts()
        dict_results = self._dicts_to_dict(key_field="id_release", lst_dicts=lst_results)
        return dict_results

    def _genres(self, str_release_ids: str) -> dict:
        sql = f"""
            SELECT
                ci.id_release,
                rg.name_genre
            FROM collection_items ci
            INNER JOIN release_genres rg
            ON rg.id_release = ci.id_release
            WHERE ci.id_release IN ({str_release_ids})
        """
        lst_results = self.read_sql(sql=sql).to_dicts()
        dict_results = self._dicts_to_dict(key_field="id_release", lst_dicts=lst_results)
        return dict_results

    def _styles(self, str_release_ids: str) -> dict:
        sql = f"""
            SELECT
                ci.id_release,
                rs.name_style
            FROM collection_items ci
            INNER JOIN release_styles rs
            ON rs.id_release = ci.id_release
            WHERE ci.id_release IN ({str_release_ids})
        """
        lst_results = self.read_sql(sql=sql).to_dicts()
        dict_results = self._dicts_to_dict(key_field="id_release", lst_dicts=lst_results)
        return dict_results