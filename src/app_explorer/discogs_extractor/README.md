# Discogs ETL

## Data extraction and enrichment process

To create insights from your music collection, first the data needs to be extracted from Discogs and stored in a database. The first step in this process is allowing the script to extract your data by using an authentication mechanism. Then we kick of the data collection part that is described below in the _Discogs ETL_ section. After this extraction process,

```mermaid
flowchart LR
    Discogs_Auth["`
        _**Discogs**_
        Authentication
    `"]
    Discogs_ETL["`
        _**Discogs**_
        Start
    `"]
    ETL_Collection["Collection"]
    ETL_CollectionItem["Collection Item"]
    ETL_Release["Release"]
    ETL_Artist["Artist"]
    ETL_Artist_Masters["Artist Masters"]
    Derive_Artist_Group["Is band indicator"]
    Derive_Artist_Collection["\# Collection items"]
    Derive_Artist_Relations["Relationships between artists"]
    Derive_Release_Roles["Artist's relationship to a release"]
    Discogs_Auth --> Discogs_ETL
    Discogs_ETL --> ETL_Collection
    Discogs_ETL --> Derive_Artist_Group
    subgraph Derive from extracted data
        direction LR
        subgraph Artist
            Derive_Artist_Group --> Thumbnail
            Thumbnail --> Derive_Artist_Collection
            Derive_Artist_Collection --> Derive_Artist_Relations
        end
        subgraph Release
            Derive_Artist_Relations --> Derive_Release_Roles
        end
        subgraph Artist network
            Derive_Release_Roles --> Me
        end
    end
    subgraph Discogs ETL
        direction LR
        ETL_Collection --> ETL_CollectionItem
        ETL_CollectionItem --> ETL_Release
        ETL_Release --> ETL_Artist
        ETL_Artist --> ETL_Artist_Masters
        ETL_Artist_Masters --> ETL_Collection
    end


```

## Class diagram

To achieve the extraction and deriving information process described above the following classes are implemented.

```mermaid
classDiagram
    Discogs *-- SecretsYAML
    Discogs *-- ETLCollection
    Discogs *-- DiscogsDerive
    DiscogsDerive *-- DeriveArtist
    DiscogsDerive *-- DeriveRelease
    DiscogsDerive *-- DeriveNetwork
    DBStorage <-- DeriveArtist
    DBStorage <-- DeriveRelease
    DBStorage <-- DeriveNetwork
    ETLCollection *-- ETLRelease
    ETLRelease *-- ETLArtists
    DiscogsETL *-- DBStorage
    DiscogsETL <-- ETLCollection
    DiscogsETL <-- ETLMaster
    ETLMaster <-- ETLRelease
    DiscogsETL <-- ETLArtists
    note for Discogs "Discogs authentication and kicking off ETL process"
    note for DiscogsETL "Contains bare bones for any ETL process"
    note for DiscogsDerive "Kicks off data derivation based on loaded Discogs data"
```

## Storage diagram

This section describes the data storage in a diagram.

```mermaid
erDiagram

    collection_value
    collection_items ||--o{ master_stats : has
    collection_items ||--o{ master_styles: has
    collection_items ||--o{ master_genres : has
    collection_items ||--o{ master_tracks: has
    collection_items ||--o{ master_videos: has
    master_tracks ||--o{ master_track_artists: "performs on"
    artist ||--o{ artist_images: "depicted in"
    artist ||--o{ artist_masters: has
    artist ||--o{ artist_groups : "part of"
    artist ||--o{ artist_aliases : "aka"
    artist ||--o{ artist_members : "group members"
    artist ||--o{ artist_urls : "info on"
    artist_masters ||--o{ master_stats : has
    artist_masters ||--o{ master_styles: has
    artist_masters ||--o{ master_genres : has
    artist_masters ||--o{ master_tracks: contains
    artist_masters ||--o{ master_videos: associated
```

```mermaid
erDiagram
    collection_value
    collection_items ||--|| release : is
    release ||--o{ release_credits : has
    release ||--o{ release_artists : has
    release ||--o{ release_labels : has
    release ||--o{ release_formats : has
    release ||--o{ release_genres : has
    release ||--o{ release_styles : has
    release ||--o{ release_tracks : has
    release ||--o{ release_videos : has
    role ||--o{ release_credits :role
    release_tracks ||--o{ release_track_artists : performs
    artist ||--o{ release_track_artists: "performs on"
    master ||--|| release: isa
    master ||--o{ master_styles: has
    master ||--o{ master_genres : has
    master ||--o{ master_tracks: has
    master ||--o{ master_videos: has
    master_tracks ||--o{ master_track_artists: "performs on"
    artist ||--o{ master_track_artists: "performs on"
    artist ||--o{ artist_masters: has
    artist ||--o{ artist_images: "depicted in"
    artist ||--o{ artist_groups : "part of"
    artist ||--o{ artist_aliases : "aka"
    artist ||--o{ artist_members : "group members"
    artist ||--o{ artist_urls : "info on"
    artist ||--o{ artist_relations : "part of"
    artist ||--o{ artist_vertex : "as"
    master ||--o{ artist_masters : ""
```

## Discogs authentication flow

```mermaid
sequenceDiagram
    participant User
    participant App
    participant Discogs

    User->>App: Navigates to /config
    App->>App: Renders config.html (credentials_ok=false)
    User->>App: Clicks "Allow Discogs access" (/get-user-access)
    App->>Discogs: request_user_access(callback_url)
    Discogs-->>App: Returns authorization URL
    App->>User: Redirects to Discogs authorization URL
    User->>Discogs: Authorizes App
    Discogs-->>User: Redirects to App's /receive-token with oauth_verifier
    User->>App: Navigates to /receive-token?oauth_verifier=...
    App->>Discogs: save_user_token(oauth_verifier)
    Discogs-->>App: Returns confirmation
    App->>User: Redirects to /config (credentials_ok=true)
```
