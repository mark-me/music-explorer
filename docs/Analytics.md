# Analytics

## Artist

The ```Artists``` class, which is responsible for retrieving and analyzing artist data from a database. It provides various methods to access artist information, including individual artist details, lists of all artists, top artists, random samples, search functionality, and similar artists based on shared genres and styles. The class uses the polars library for data manipulation and inherits from a DBStorage class for database interaction.

### Key Components

* **```Artists``` Class**: The core component of this file. It handles all interactions with the database related to artist data. It inherits from ```DBStorage``` to manage database connections and queries.
* **```artist(self, id_artist: int)```**: Retrieves detailed information for a specific artist by ID. This includes basic information, URLs, and similar artists.
* **```all(self)```**: Retrieves a list of all artists in the database, ordered alphabetically.
* **```all_top_10(self)```**: Retrieves the first 10 artists, ordered alphabetically.
* **```top_collected(self)```**: Retrieves artists sorted by the number of collected items, descending.
* **```random(self, qty_sample: int = 20)```**: Retrieves a random sample of artists.
* **```search(self, text_search: str)```**: Searches for artists whose names match the provided search text.
* **```similar_genre_style(self, id_artist: int)```**: Finds similar artists based on shared genres and styles, using Jaccard similarity.
* **```_add_nested_information(self, lst_artists: list)```**: A private helper method that enriches artist data with related information like formats, genres, styles, and group memberships.
* **Private Helper Methods** (e.g., ```_formats```, ```_genres```, ```_styles```): These methods retrieve specific related data for artists, such as their associated formats, genres, and styles. They are used by ```_add_nested_information``` to build a comprehensive artist object.
* **```polars``` Library**: Used for efficient data manipulation and querying. The Artists class uses polars DataFrames to process and organize the retrieved data.
* **```DBStorage``` Class**: Although not defined in this file, it's a crucial base class providing database connectivity and query execution functionalities. The ```Artists``` class relies on it to interact with the database.

## Collection

The ```Collection``` class, which is responsible for retrieving and processing data about a user's music collection from a database. It provides methods to access various aspects of the collection, such as all items, a random sample, search results, items by artist, and format counts. The class uses the ```polars``` library for data manipulation and inherits from a ```DBStorage``` class for database interaction.

### Key components

* **```Collection``` Class**: The core component of this file. It handles database queries and data processing related to the music collection. It inherits from ```DBStorage``` to manage database connections.
* **```all()``` Method**: Retrieves all collection items, ordered alphabetically, with nested information about artists, formats, genres, and styles.
* **```all_top_10()``` Method**: Retrieves the first 10 collection items alphabetically, similar to all() but limited to 10 results.
* **```random()``` Method**: Retrieves a random sample of collection items with nested information.
* **```search()``` Method**: Searches for collection items by title, returning matching items with nested information.
* **```artist()``` Method**: Retrieves collection items by a specific artist ID, including track information.
* **```formats()``` Method**: Retrieves the counts of different formats in the collection.
* _add_nested_information() Method: A private helper method that enriches collection items with related artists, formats, genres, and styles.
* **```_artists()```, ```_formats()```, ```_genres()```, ```_styles()``` Methods**: Private helper methods to retrieve specific related information for releases. These methods use SQL queries to fetch data and organize it into dictionaries for easy access.
* **Use of ```polars```**: The code utilizes the ```polars``` library for efficient data manipulation and querying, converting between DataFrames and lists of dictionaries.

## Releases

Two classes, ```Releases``` and ```Release```, interact with a music database. ```Releases``` provides a method to fetch data for a specific release by ID, while ```Release``` handles retrieving detailed information about a single release, including its artists, videos, tracks, formats, labels, genres, and styles. Both classes inherit from a DBStorage class (not shown in the provided code), which presumably handles database connection and query execution.

### Key components

* **```Releases``` Class**: This class acts as a high-level interface for accessing release data. Its primary function is get_release(id_release), which takes a release ID and returns a dictionary containing comprehensive information about the release.
* **```Release``` Class**: This class is responsible for retrieving detailed information about a specific release. It uses the provided id_release during initialization and offers several methods to fetch different aspects of the release data:
* **```data()```**: Returns a comprehensive dictionary containing all details about the release.
* **```release()```**: Retrieves basic release information (title, year, etc.).
* **```artists()```**: Retrieves associated artists.
* **```videos()```**: Retrieves associated videos.
* **```tracks()```**: Retrieves associated tracks.
* **```formats()```**: Retrieves release formats (e.g., Vinyl, CD).
* **```labels()```**: Retrieves associated record labels.
* **```genres()```**: Retrieves associated genres.
* **```styles()```**: Retrieves associated styles.
* **```DBStorage``` Class (External)**: Although not defined in this file, it's a crucial component. Both ```Releases``` and ```Release``` inherit from it, suggesting it handles database interaction logic. The ```read_sql()``` method used within ```Release``` likely comes from this parent class.

