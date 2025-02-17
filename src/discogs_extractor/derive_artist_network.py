import igraph as ig
import polars as pl

from db_operations import DBStorage


class DeriveArtistNetwork(DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)

    def process(self) -> None:
        self._create_artist_vertices()
        self._create_artist_edges()
        self._create_clusters()
        self._create_community_dendrogram()

    def _create_artist_vertices(self) -> None:
        """Retrieve artists in order to determine where to stop discogs extraction"""
        self.drop_existing_table(name_table="artist_vertex")
        sql_statement = """
            CREATE TABLE artist_vertex AS
                SELECT id_artist, MAX(in_collection) AS in_collection
                FROM (  SELECT id_artist, IIF(qty_collection_items > 0, 1, 0) AS in_collection FROM artist
                            UNION
                        SELECT id_alias, 0 as in_collection from artist_aliases
                            UNION
                        SELECT id_member, 0 FROM artist_members
                            UNION
                        SELECT id_group, 0 FROM artist_groups
                            UNION
                        SELECT id_artist, 0 FROM artist_masters WHERE role IN ('Main', 'Appearance', 'TrackAppearance')
                            UNION
                        SELECT release_artists.id_artist, MAX(IIF(date_added IS NULL, 0, 1))
                        FROM release_artists
                        INNER JOIN release
                            ON release.id_release = release_artists.id_release
                        LEFT JOIN collection_items
                            ON collection_items.id_release = release.id_release
                        GROUP BY release_artists.id_artist )
                GROUP BY id_artist"""
        self.execute_sql(sql=sql_statement)

    def _create_artist_edges(self) -> None:
        """Retrieve artist cooperations in order to determine where to stop discogs extraction"""
        self.drop_existing_table(name_table="artist_edge")
        sql_statement = """
            CREATE TABLE artist_edge AS
                SELECT DISTINCT id_artist_from, id_artist_to, relation_type
                FROM (  SELECT id_member AS id_artist_from, id_artist as id_artist_to, 'group_member' as relation_type
                        FROM artist_members
                    UNION
                        SELECT id_artist, id_group, 'group_member' FROM artist_groups
                    UNION
                        SELECT a.id_alias, a.id_artist, 'artist_alias'
                        FROM artist_aliases a
                        LEFT JOIN artist_aliases b
                            ON a.id_artist = b.id_alias AND
                                a.id_alias = b.id_artist
                        WHERE a.id_artist > b.id_artist OR b.id_artist IS NULL
                    UNION
                        SELECT a.id_artist, b.id_artist, 'co_appearance'
                        FROM release_artists a
                        INNER JOIN release_artists b
                            ON b.id_release = a.id_release
                        WHERE a.id_artist != b.id_artist )
                GROUP BY id_artist_from, id_artist_to, relation_type;"""
        self.execute_sql(sql=sql_statement)

    def _extract_artist_to_ignore(self) -> None:
        """Define which artists to exclude from discogs extraction"""
        df_vertices = self.read_table(name_table="artist_vertex")
        df_edges = self.read_table(name_table="artist_edge")
        graph = ig.Graph.DataFrame(
            edges=df_edges,
            directed=False,
            vertices=df_vertices,
        )
        # Select relevant vertices
        vtx_collection = graph.vs.select(in_collection_eq=1)
        vtx_relevant = []
        for vtx in vtx_collection:
            vtx_neighbors = graph.neighborhood(
                vertices=vtx, order=2
            )  # Only query those that have less than 3 steps
            vtx_neighbors = list(set(vtx_neighbors))
            vtx_relevant = list(set(vtx_neighbors + vtx_relevant))
            vtx_connectors = graph.get_shortest_paths(
                vtx, to=vtx_collection
            )  # Vertices that connect artists in the collection
            vtx_connectors = [x for l in vtx_connectors for x in l]
            vtx_relevant = list(set(vtx_connectors + vtx_relevant))
        # Get vertices to ignore
        vtx_to_exclude = list(set(graph.vs.indices) - set(vtx_relevant))
        df_ignore = pl.DataFrame({"id_artist": graph.vs[vtx_to_exclude]["name"]})
        self.store_replace(df=df_ignore, name_table="artist_ignore")

    def _get_artist_graph(self) -> None:
        lst_edges = self.read_sql(
            sql="SELECT * FROM artist_relations WHERE id_artist_from != id_artist_to"
        ).to_dict(orient="records")
        lst_vertices = self.read_sql(
            sql="SELECT *, IIF(qty_collection_items > 0, 1, 0) AS in_collection FROM artist"
        ).to_dict(orient="records")
        graph = ig.Graph.DictList(
            vertices=lst_vertices,
            edges=lst_edges,
            vertex_name_attr="id_artist",
            edge_foreign_keys=("id_artist_from", "id_artist_to"),
        )
        vtx_collection = graph.vs.select(in_collection_eq=1)
        vtx_connectors_all = []
        for vtx in vtx_collection:
            # Only query those that have less than 3 steps
            vtx_neighbors = graph.neighborhood(vertices=vtx, order=2)
            vtx_neighbors = list(set(vtx_neighbors))
            vtx_connectors_all = list(set(vtx_neighbors + vtx_connectors_all))
            # Integrate connectors
            vtx_connectors = graph.get_shortest_paths(vtx, to=vtx_collection)
            vtx_connectors = [x for l in vtx_connectors for x in l]
            vtx_connectors_all = list(set(vtx_connectors + vtx_connectors_all))
        vtx_to_exclude = list(set(graph.vs.indices) - set(vtx_connectors_all))
        graph.delete_vertices(vtx_to_exclude)
        return graph

    def _extract_artist_to_ignore(self) -> None:
        """Define which artists to exclude from discogs extraction

        Pruning the graph to avoid pulling too much Discogs information, that is so far
        in the tree from collected artists to be relevant.
        """
        lst_vertices = self.read_table(name_table="artist_vertex").to_dict(
            orient="records"
        )
        lst_edges = self.read_table(name_table="artist_edge").to_dict(orient="records")
        graph = ig.Graph.DictList(
            vertices=lst_vertices,
            edges=lst_edges,
            vertex_name_attr="id_artist",
            edge_foreign_keys=("id_artist_from", "id_artist_to"),
        )
        # Select relevant vertices
        vtx_collection = graph.vs.select(in_collection_eq=1)
        vtx_relevant = []
        for vtx in vtx_collection:
            vtx_neighbors = graph.neighborhood(
                vertices=vtx, order=2
            )  # Only query those that have less than 3 steps
            vtx_neighbors = list(set(vtx_neighbors))
            vtx_relevant = list(set(vtx_neighbors + vtx_relevant))
            vtx_connectors = graph.get_shortest_paths(
                vtx, to=vtx_collection
            )  # Vertices that connect artists in the collection
            vtx_connectors = [x for l in vtx_connectors for x in l]
            vtx_relevant = list(set(vtx_connectors + vtx_relevant))
        # Get vertices to ignore
        vtx_to_exclude = list(set(graph.vs.indices) - set(vtx_relevant))
        df_ignore = pl.DataFrame({"id_artist": graph.vs[vtx_to_exclude]["name"]})
        self.store_replace(df=df_ignore, name_table="artist_ignore")

    def _cluster_component(self, graph_component: ig.Graph) -> pl.DataFrame:
        idx_community_start = 0
        graph_component.vs["id_community_from"] = idx_community_start
        # Queue for processing graphs, keeping track level in community tree and relationships between branches
        lst_processing_queue = [{"graph": graph_component, "tree_level": 0}]
        # For each sub graph until none in the list
        qty_graphs_queued = len(
            lst_processing_queue
        )  # Number of graphs in the community tree
        lst_communities = []  # Data-frames with data for a processed graph
        while qty_graphs_queued > 0:
            dict_queue = lst_processing_queue.pop(0)
            graph = (
                dict_queue.get("graph")
            ).simplify()  # Remove self referential and double links
            tree_level = dict_queue.get("tree_level")
            qty_collection_items = sum(graph.vs["in_collection"])
            if (
                qty_collection_items > 2
            ):  # Only determine communities if the number of vertices is higher than x
                cluster_hierarchy = graph.community_fastgreedy()  # communities
                # Setting maximum and minimum of number of clusters
                qty_clusters = (
                    15
                    if cluster_hierarchy.optimal_count > 15
                    else cluster_hierarchy.optimal_count
                )
                qty_clusters = qty_clusters if qty_clusters > 2 else 2
                cluster_communities = cluster_hierarchy.as_clustering(
                    n=qty_clusters
                )  # Determine communities
                community_membership = cluster_communities.membership
                communities = set(community_membership)
                eigenvalue = []
                for community in communities:
                    graph_sub = cluster_communities.subgraph(community)
                    graph_sub.vs["id_community_from"] = [
                        community + (idx_community_start + 1)
                    ] * len(graph_sub.vs)
                    lst_processing_queue.append(
                        {"graph": graph_sub.copy(), "tree_level": tree_level + 1}
                    )
                    eigenvalue = eigenvalue + graph_sub.eigenvector_centrality(
                        directed=False
                    )  # Calculate eigenvalue per sub_graph
                # Make sure community numbers are unique
                community_membership = [
                    i + (idx_community_start + 1) for i in community_membership
                ]
                idx_community_start = max(community_membership)
                lst_communities.append(
                    pl.DataFrame(
                        {
                            "id_artist": graph.vs["id_artist"],
                            "name_artist": graph.vs["name_artist"],
                            "in_collection": graph.vs["in_collection"],
                            "qty_collection_items": graph.vs["qty_collection_items"],
                            "id_hierarchy": [tree_level] * len(graph.vs),
                            "id_community_from": graph.vs["id_community_from"],
                            "id_community": community_membership,
                            "eigenvalue": eigenvalue,
                        }
                    )
                )
            qty_graphs_queued = len(lst_processing_queue)
            df_communities = pl.concat(lst_communities, axis=0, ignore_index=True)
        return df_communities

    def _create_clusters(self) -> None:
        graph_all = self._get_artist_graph()
        # Cluster all components
        lst_components = graph_all.decompose()  # Decompose graph
        lst_dendrogram = []
        for component in lst_components:
            if sum(component.vs["in_collection"]) <= 2:
                qty_vertices = len(component.vs)
                df_dendrogram = pl.DataFrame(
                    {
                        "id_artist": component.vs["id_artist"],
                        "name_artist": component.vs["name_artist"],
                        "in_collection": component.vs["in_collection"],
                        "qty_collection_items": component.vs["qty_collection_items"],
                        "id_hierarchy": [0] * qty_vertices,
                        "id_community_from": [0] * qty_vertices,
                        "id_community": [1] * qty_vertices,
                        "eigenvalue": component.eigenvector_centrality(directed=False),
                    }
                )
            else:
                df_dendrogram = self._cluster_component(component)
            lst_dendrogram.append(df_dendrogram)
        # Making all community id's unique across the dendrograms and add root to connect to components
        community_max = 0
        for i in range(len(lst_dendrogram)):
            df_component = lst_dendrogram[i]
            df_component["id_community"] = (
                df_component["id_community"] + community_max + 1
            )
            # If a component has multiple sub communities add an extra vertex so they will not be added to the root directly
            if len(set(df_component["id_community_from"])) > 1:
                df_component.loc[:, "id_community_from"] = (
                    df_component.loc[:, "id_community_from"] + community_max + 1
                )
                df_root = df_component.loc[df_component["id_hierarchy"] == 0].copy()
                df_root["id_community"] = df_root["id_community_from"]
                df_root["id_community_from"] = 0
                df_component.loc[:, "id_hierarchy"] = (
                    df_component.loc[:, "id_hierarchy"] + 1
                )
                df_component = pl.concat(
                    [df_component, df_root], axis=0, ignore_index=True
                )
            community_max = max(df_component["id_community"])
            lst_dendrogram[i] = df_component
        df_hierarchy = pl.concat(lst_dendrogram, axis=0, ignore_index=True)
        self.store_replace(df=df_hierarchy, name_table="artist_community_hierarchy")

    def _create_community_labels(self) -> None:
        self.drop_existing_table(name_table="artist_collection_ranked_eigenvalue")
        sql_statement = """
            CREATE TABLE artist_collection_ranked_eigenvalue AS
                SELECT id_community,
                    name_artist,
                    ROW_NUMBER() OVER (PARTITION BY id_community ORDER BY eigenvalue DESC) AS rank_eigenvalue
                FROM artist_community_hierarchy
                WHERE in_collection = 1;
        """
        self.execute_sql(sql=sql_statement)
        self.drop_existing_table(name_table="community_collection_label")
        sql_statement = """
            CREATE TABLE community_collection_label AS
                SELECT id_community,
                    GROUP_CONCAT(name_artist) AS label_community_collection
                FROM artist_collection_ranked_eigenvalue
                WHERE rank_eigenvalue <= 3
                GROUP BY id_community;
        """
        self.execute_sql(sql=sql_statement)
        self.drop_existing_table(name_table="artist_ranked_eigenvalue")
        sql_statement = """
            CREATE TABLE artist_ranked_eigenvalue AS
                SELECT id_community,
                    name_artist,
                    ROW_NUMBER() OVER (PARTITION BY id_community ORDER BY eigenvalue DESC) AS rank_eigenvalue
                FROM artist_community_hierarchy;
        """
        self.execute_sql(sql=sql_statement)
        self.drop_existing_table(name_table="community_label")
        sql_statement = """
            CREATE TABLE community_label AS
                SELECT id_community,
                    GROUP_CONCAT(name_artist) AS label_community
                FROM artist_ranked_eigenvalue
                WHERE rank_eigenvalue <= 3
                GROUP BY id_community;
        """
        self.execute_sql(sql=sql_statement)

    def _create_community_dendrogram(self) -> None:
        self._create_community_labels()
        self.drop_existing_table(name_table="community_dendrogram_vertices")
        sql_statement = """
            CREATE TABLE community_dendrogram_vertices AS
                SELECT a.id_community,
                    id_hierarchy AS id_hierarchy,
                    label_community,
                    label_community_collection,
                    SUM(in_collection) AS qty_artists_collection,
                    COUNT(*) as qty_artists
                FROM artist_community_hierarchy a
                LEFT JOIN community_label  b
                    ON b.id_community = a.id_community
                LEFT JOIN community_collection_label c
                    ON c.id_community = a.id_community
                GROUP BY a.id_community, id_hierarchy
                UNION
                SELECT 0 as id_community,
                    0 AS id_hierarchy,
                    label_community,
                    label_community_collection,
                    SUM(in_collection) AS qty_artists_collection,
                    COUNT(*) as qty_artists
                FROM artist_community_hierarchy a
                LEFT JOIN community_label  b
                    ON b.id_community = a.id_community
                LEFT JOIN community_collection_label c
                    ON c.id_community = a.id_community
                WHERE a.id_hierarchy = 0;
        """
        self.execute_sql(sql=sql_statement)
        self.drop_existing_table(name_table="community_dendrogram_edges")
        sql_statement = """
            CREATE TABLE community_dendrogram_edges AS
                SELECT id_community_from as id_from,
                    id_community as id_to, id_hierarchy,
                    MAX(in_collection) AS to_collection_artists
                FROM artist_community_hierarchy
                GROUP BY id_community_from,
                    id_community,
                    id_hierarchy;
        """
        self.execute_sql(sql=sql_statement)

    def artists_from_group_and_membership(self) -> None:
        """Process artist information derived from groups and memberships"""
        # db_reader = _db_reader.Collection(db_file=self.db_file)
        # db_writer = _db_writer.Collection(db_file=self.db_file)
        self._extract_artist_to_ignore()
        qty_artists_not_added = self.read_sql(
            sql="SELECT COUNT(*) as qty FROM vw_artists_not_added;"
        )[0]["qty"]
        while qty_artists_not_added > 0:
            df_write_attempts = self.read_table(name_table="artist_write_attempts")
            df_artists_new = self.read_table(name_table="vw_artists_not_added")
            artists = []
            for index, row in df_artists_new.iterrows():
                artists.append(self.client_discogs.artist(id=row["id_artist"]))
                df_write_attempts = pl.concat(
                    [
                        df_write_attempts,
                        pl.DataFrame.from_records(
                            [{"id_artist": row["id_artist"], "qty_attempts": 1}]
                        ),
                    ]
                )
                # df_write_attempts = df_write_attempts.append({'id_artist': row['id_artist'], 'qty_attempts': 1}, ignore_index=True)
            derive = _derive.Artists(artists=artists, db_file=self.file_db)
            derive.process_masters = False
            derive.process()
            self._extract_artist_to_ignore()
            df_write_attempts = (
                df_write_attempts.groupby(["id_artist"])["qty_attempts"]
                .sum()
                .reset_index()
            )
            db_writer.artist_write_attempts(df_write_attempts=df_write_attempts)
            qty_artists_not_added = self.read_sql(
                sql="SELECT COUNT(*) as qty FROM vw_artists_not_added;"
            )[0]["qty"]
