# Bitdex V2 - Storage Architecture Overhaul
We are migrating Bitdex from redb-based storage to a fully filesystem-oriented architecture. This document describes the updated configuration schema reflecting those changes.
Context: The original V2 design used redb as an embedded KV store for bitmap persistence, with a two-tier memory model (ArcSwap snapshot for hot bitmaps, moka LRU cache for cold bitmaps). Under load testing at 104M records, redb produced a database file roughly 2x larger than the raw source data, had no built-in compression, and the two-tier cache added significant application complexity.
Key changes being implemented:
Drop redb entirely. All bitmaps are stored as individual files on the filesystem, one file per bitmap. Reads are memory-mapped — the OS page cache replaces the moka LRU cache entirely. Hot bitmaps stay warm automatically. Cold bitmaps load via page faults at NVMe speed. No application-level cache management needed.
Drop moka and the two-tier storage split. The snapshot/cached tier distinction is gone. All bitmaps are equal — the OS decides what lives in RAM based on access patterns. This removes a significant chunk of complexity from the codebase.
Add filesystem-based document storage. Documents stored as msgpack + zstd, sharded by hex slot ID. Expected ~100-150 bytes per document vs ~600 bytes raw JSON. Enables PATCH operations without callers supplying old values.
Add document schema optimization. Default value elision removes fields matching their schema default before serialization. Schema versioning allows defaults to change over time with lazy migration. Low cardinality strings auto-build integer dictionaries with configurable storage width (u8/u16/u32).
Add backpressure-driven write pipeline. No manual loading mode. The system auto-detects bulk load vs steady state via mutation channel depth and adjusts batch sizes, publish frequency, and merge intervals automatically.
Cache system updated. Trie filter caches and bound caches are now persisted to disk alongside source bitmaps. Caches survive restarts — no cold start penalty. Application-level LRU replaced by periodic GC based on exponential decay hit rates.
The attached [config schema](docs\in\config-schema-v2.md) reflects all of these changes. Use it as the reference for updating the implementation.

## Doc changes

Current docs are shaped like:
```
{
    "id": 122580067,
    "reactionCount": 1,
    "commentCount": 0,
    "collectedCount": 0,
    "index": 14,
    "postId": 26904708,
    "url": "63946da7-80b4-4a66-ba39-9706f4d38d53",
    "nsfwLevel": 1,
    "aiNsfwLevel": 0,
    "width": 1216,
    "height": 832,
    "hash": "UOB:v#_4-;xuD%M{NGWAMxRjRjR*xuM{Rij[",
    "hideMeta": false,
    "sortAt": 1772214735,
    "type": "image",
    "userId": 1850035,
    "needsReview": null,
    "hasMeta": true,
    "onSite": false,
    "postedToId": 2727392,
    "combinedNsfwLevel": 1,
    "baseModel": "NoobAI",
    "modelVersionIds": [
        1470451,
        1470501,
        1833199,
        2279272,
        2409201,
        2442886
    ],
    "toolIds": [],
    "techniqueIds": [],
    "publishedAtUnix": 1772214735313,
    "existedAtUnix": 1772215278068,
    "sortAtUnix": 1772214735313,
    "tagIds": [
        2309,
        6634,
        9090,
        111777,
        111807,
        111839,
        111915,
        112143,
        112144,
        112172,
        112359,
        116352,
        122842,
        143215,
        234268
    ],
    "modelVersionIdsManual": [
        2727392
    ],
    "minor": false,
    "blockedFor": null,
    "remixOfId": null,
    "hasPositivePrompt": true,
    "availability": "Public",
    "poi": false,
    "acceptableMinor": false,
    "publishedAt": 1772214735313
}
```

I want the docs that we store to be like:
```
{
    "id": 122580067,
    "reactionCount": 1, // default 0
    "commentCount": 0, // default 0
    "collectedCount": 0, // default 0
    "index": 14,
    "postId": 26904708,
    "url": "63946da7-80b4-4a66-ba39-9706f4d38d53",
    "nsfwLevel": 1,
    "width": 1216,
    "height": 832,
    "hash": "UOB:v#_4-;xuD%M{NGWAMxRjRjR*xuM{Rij[",
    "hideMeta": false,  // default false
    "publishedAt": 1772214735313, // shrink dates to timestamp (no ms)
    "sortAt": 1772214735, // shrink dates to timestamp (no ms)
    "type": "image", // low cardinality string
    "userId": 1850035,
    "needsReview": null,  // low cardinality string, default null
    "hasMeta": true, // default true
    "onSite": false, // default false
    "postedToId": 2727392,
    "baseModel": "NoobAI", // low cardinality string
    "modelVersionIds": [
        1470451,
        1470501,
        1833199,
        2279272,
        2409201,
        2442886
    ], // default []
    "toolIds": [], // default []
    "techniqueIds": [], // default []
    "tagIds": [
        2309,
        6634,
        9090,
        111777,
        111807,
        111839,
        111915,
        112143,
        112144,
        112172,
        112359,
        116352,
        122842,
        143215,
        234268
    ], // default []
    "modelVersionIdsManual": [
        2727392
    ], // default []
    "minor": false, // default false
    "blockedFor": null,  // low cardinality string, default null
    "remixOfId": null, // default null
    "hasPositivePrompt": true, // default true
    "availability": "Public", // low cardinality string, default Public
    "poi": false, // default false
    "acceptableMinor": false, // default false
}
```
Notes:
- Some fields have been removed completely
- There are a bunch of defaults that I want to elide since they are defaults
- enums and things with limited result sets should be stored as numbers as low card strings for example:
```
fields:
  - name: type
    field_type: low_cardinality_string
    cardinality: u8    # max 255 distinct values

  - name: baseModel
    field_type: low_cardinality_string
    cardinality: u8    # handful of base models

  - name: availability
    field_type: low_cardinality_string
    cardinality: u8    # Public, Private, Unsearchable

  - name: needsReview
    field_type: low_cardinality_string
    nullable: true
    default: null
    cardinality: u8    # small set of review states

  - name: blockedFor
    field_type: low_cardinality_string
    nullable: true
    default: null
    cardinality: u8

  - name: someHigherCardField
    field_type: low_cardinality_string
    cardinality: u16   # up to 65k distinct values
```
- Notice that the values aren't predefined. Instead they're automatically added to dict docs

So ultimately after elides, msgpack, and compression, things should be much much smaller:
```
compressed(msgpack({
    "id": 122580067,
    "reactionCount": 1, // default 0
    "index": 14,
    "postId": 26904708,
    "url": "63946da7-80b4-4a66-ba39-9706f4d38d53",
    "nsfwLevel": 1,
    "width": 1216,
    "height": 832,
    "hash": "UOB:v#_4-;xuD%M{NGWAMxRjRjR*xuM{Rij[",
    "hideMeta": false,  // default false
    "publishedAt": 1772214735, // shrink dates to timestamp (no ms)
    "sortAt": 1772214735, // shrink dates to timestamp (no ms)
    "type": 1, // low cardinality string
    "userId": 1850035,
    "postedToId": 2727392,
    "baseModel": 12, // low cardinality string
    "modelVersionIds": [
        1470451,
        1470501,
        1833199,
        2279272,
        2409201,
        2442886
    ], // default []
    "tagIds": [
        2309,
        6634,
        9090,
        111777,
        111807,
        111839,
        111915,
        112143,
        112144,
        112172,
        112359,
        116352,
        122842,
        143215,
        234268
    ], // default []
    "modelVersionIdsManual": [
        2727392
    ], // default []
}))
```

