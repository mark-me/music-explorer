import polars as pl

from db_operations import DBStorage


class Artists(DBStorage):
    def __init__(self, file_db, schema="main"):
        super().__init__(file_db, schema)
        self.sql_all = """
            SELECT
                a.id_artist,
                a.name_artist,
                a.profile,
                img.url_image,
                img.url_image_150,
                img.width_image,
                COUNT(DISTINCT rf.id_release) AS qty_collection_items
            FROM collection.main.artist a
            LEFT JOIN collection.main.artist_images as img
            ON img.id_artist = a.id_artist
            LEFT JOIN collection.main.release_artists ra
            ON ra.id_artist = a.id_artist
            INNER JOIN collection.main.collection_items ci
            ON ci.id_release = ra.id_release
            LEFT JOIN collection.main.release_formats rf
            ON rf.id_release = ci.id_release
            WHERE ( img.type = 'primary' OR img.type IS NULL )
            AND (rf.name_format = 'Vinyl' or rf.name_format IS NULL)
            GROUP BY
                a.id_artist,
                a.name_artist,
                a.profile,
                img.url_image,
                img.url_image_150,
                img.width_image
        """

    def artist(self, id_artist: int) -> dict:
        df = self.read_sql(sql=self.sql_all)
        lst_artists = df.filter(pl.col("id_artist") == id_artist).to_dicts()
        lst_artists = self._add_nested_information(lst_artists=lst_artists)
        lst_artists = lst_artists[0]
        # Add urls
        sql_urls = f"SELECT url FROM artist_urls WHERE id_artist={id_artist}"
        lst_urls = self.read_sql(sql=sql_urls).to_dicts()
        lst_artists.update({"urls": lst_urls})
        # Add similar artists based on genre and style
        lst_artists_similar = self.similar_genre_style(id_artist=id_artist)
        lst_artists.update({"artists_similar": lst_artists_similar})
        return lst_artists

    def all(self) -> list:
        sql = (
            self.sql_all
            + """
        ORDER BY
            LTRIM(
                LTRIM(
                    LTRIM(
                    UPPER(a.name_artist),
                    '.'),
                    ''''
                    ), '"'
                    )"""
        )
        lst_artists = self.read_sql(sql=sql).to_dicts()
        lst_artists = self._add_nested_information(lst_artists=lst_artists)
        return lst_artists

    def all_top_10(self) -> list:
        sql = (
            self.sql_all
            + """
        ORDER BY
            LTRIM(
                LTRIM(
                    LTRIM(
                    UPPER(a.name_artist),
                    '.'),
                    ''''
                    ), '"'
                    ) LIMIT 10"""
        )
        lst_artists = self.read_sql(sql=sql).to_dicts()
        lst_artists = self._add_nested_information(lst_artists=lst_artists)
        return lst_artists

    def top_collected(self) -> list:
        df = self.all()
        df.sort("qty_collection_items", descending=True)
        lst_artists = df.to_dicts()
        lst_artists = self._add_nested_information(lst_artists=lst_artists)
        return lst_artists

    def random(self, qty_sample: int = 20) -> list:
        df = self.read_sql(sql=self.sql_all)
        lst_artists = df.sample(n=qty_sample).to_dicts()
        lst_artists = self._add_nested_information(lst_artists=lst_artists)
        return lst_artists

    def similar_genre_style(self, id_artist: int) -> list:
        sql_similarity_threshold = f"""
            SELECT
                AVG((asg.similarity_jaccard + IFNULL(ass.similarity_jaccard, 0)) / 2) as perc_similarity_genre_style
            FROM artist_similarity_genres asg
            INNER JOIN artist_similarity_styles ass
            ON ass.id_artist = asg.id_artist
            AND ass.id_artist_1 = asg.id_artist_1
            WHERE
                asg.id_artist={id_artist}
        """
        similarity_threshold = self.read_sql(sql=sql_similarity_threshold).item(0, 0)
        sql_similarity = f"""
            SELECT
                asg.id_artist_1 as id_artist,
                a.name_artist,
                img.url_image,
                img.url_image_150,
                img.width_image,
                asg.qty_all as qty_all_genres,
                asg.similarity_jaccard as perc_similarity_genres,
                IFNULL(ass.qty_all, 0) as qty_all_styles,
                IFNULL(ass.similarity_jaccard, 0) as perc_similarity_styles,
                (IFNULL(ass.similarity_jaccard, 0) + asg.similarity_jaccard)/2 as perc_similarity
            FROM artist_similarity_genres asg
            INNER JOIN artist_similarity_styles ass
            ON ass.id_artist = asg.id_artist
            AND ass.id_artist_1 = asg.id_artist_1
            INNER JOIN artist a
            ON a.id_artist = asg.id_artist_1
            LEFT JOIN collection.main.artist_images as img
            ON img.id_artist = a.id_artist
            WHERE
                ( img.type = 'primary' OR img.type IS NULL ) AND
                asg.id_artist={id_artist} AND
                (IFNULL(ass.similarity_jaccard, 0) + asg.similarity_jaccard)/2 > {similarity_threshold}
                AND EXISTS (
                    SELECT 1
                    FROM collection.main.release_artists ra
                    INNER JOIN collection.main.collection_items ci
                    ON ci.id_release = ra.id_release
                    WHERE ra.id_artist = asg.id_artist_1
                )
            ORDER BY
                asg.qty_all * perc_similarity_genres + IFNULL(ass.qty_all, 0) * IFNULL(perc_similarity_styles, 0) DESC
            LIMIT 25
        """
        lst_results = self.read_sql(sql=sql_similarity).to_dicts()
        return lst_results

    def _add_nested_information(self, lst_artists: list) -> list:
        str_artist_ids = ", ".join([str(i["id_artist"]) for i in lst_artists])
        dict_formats = self._formats(str_artist_ids=str_artist_ids)
        dict_genres = self._genres(str_artist_ids=str_artist_ids)
        dict_styles = self._styles(str_artist_ids=str_artist_ids)

        # Adding nested information
        for i, artist in enumerate(lst_artists):
            id_artist = artist["id_artist"]
            if id_artist in dict_formats:
                lst_artists[i].update({"formats_collection": dict_formats[id_artist]})
            if id_artist in dict_genres:
                lst_artists[i].update({"genres_collection": dict_genres[id_artist]})
            if id_artist in dict_styles:
                lst_artists[i].update({"styles_collection": dict_styles[id_artist]})
        return lst_artists

    def _formats(self, str_artist_ids: str) -> dict:
        sql = f"""
            SELECT
                ra.id_artist,
                rf.name_format,
                COUNT(*) AS qty_collection_items
            FROM collection_items ci
            INNER JOIN release_artists ra
            ON ra.id_release = ci.id_release
            INNER JOIN release_formats rf
            ON rf.id_release = ci.id_release
            WHERE ra.id_artist IN ({str_artist_ids})
            GROUP BY
                ra.id_artist,
                rf.name_format
        """
        lst_formats = self.read_sql(sql=sql).to_dicts()
        dict_formats = self._dicts_to_dict(key_field="id_artist", lst_dicts=lst_formats)
        return dict_formats

    def _genres(self, str_artist_ids: str) -> dict:
        sql = f"""
            SELECT
                ra.id_artist,
                rg.name_genre,
                COUNT(*) AS qty_collection_items
            FROM collection_items ci
            INNER JOIN release_artists ra
            ON ra.id_release = ci.id_release
            INNER JOIN release_genres rg
            ON rg.id_release = ci.id_release
            WHERE ra.id_artist IN ({str_artist_ids})
            GROUP BY
                ra.id_artist,
                rg.name_genre
        """
        lst_genres = self.read_sql(sql=sql).to_dicts()
        dict_genres = self._dicts_to_dict(key_field="id_artist", lst_dicts=lst_genres)
        return dict_genres

    def _styles(self, str_artist_ids: str) -> dict:
        # Formats
        sql = f"""
            SELECT
                ra.id_artist,
                rs.name_style,
                COUNT(*) AS qty_collection_items
            FROM collection_items ci
            INNER JOIN release_artists ra
            ON ra.id_release = ci.id_release
            INNER JOIN release_styles rs
            ON rs.id_release = ci.id_release
            WHERE ra.id_artist IN ({str_artist_ids})
            GROUP BY
                ra.id_artist,
                rs.name_style
        """
        lst_styles = self.read_sql(sql=sql).to_dicts()
        dict_styles = self._dicts_to_dict(key_field="id_artist", lst_dicts=lst_styles)
        return dict_styles