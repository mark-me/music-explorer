# Welcome to Music Explorer's documentation

This is the development documentation for the [music-explorer](https://github.com/mark-me/music-explorer) project.

This project scrapes a user's Discogs account and allows the user to navigate their music collection

## Key components

The project consists of:

* **```app_explorer```** - a web-app that is the user UI, that consists of:
  * [**```discogs_extractor```**](DiscogsETL.md) - an ETL package that retrieves the Discogs data, stores it in a [DuckDB](https://duckdb.org/) database and transforms it.
  * a set of analytics classes that retrieves the data from the database for use in the UI
* A set of scripts to deploy all in a Docker environment.

```bash
├── config
├── docs
│   └── assets
└── src
    ├── app_explorer
    │   ├── analytics
    │   ├── discogs_extractor
    │   ├── static
    │   │   ├── css
    │   │   ├── icons
    │   │   └── images
    │   └── templates
    │       ├── artists
    │       └── collection_items
    ├── db_operations
    ├── log_config
    └── utils
```
