#!/usr/bin/env bash
# Dump all BitDex tables from Postgres to CSV files on the PG server's local disk.
#
# Run via kubectl exec:
#   MSYS_NO_PATHCONV=1 kubectl exec -n cnpg-database cnpg-cluster-nvme0-1 -- \
#     bash /dev/stdin < tools/pg-dump-tables.sh
#
# Or copy it to the pod first:
#   kubectl cp tools/pg-dump-tables.sh cnpg-database/cnpg-cluster-nvme0-1:/tmp/pg-dump-tables.sh
#   MSYS_NO_PATHCONV=1 kubectl exec -n cnpg-database cnpg-cluster-nvme0-1 -- bash /tmp/pg-dump-tables.sh

set -euo pipefail

DB="civitai"
DUMP_DIR="/var/lib/postgresql/data/bitdex_dump"
PSQL="psql -U postgres -d $DB -c"

mkdir -p "$DUMP_DIR"

echo "=== BitDex PG Dump ==="
echo "Output: $DUMP_DIR"
echo ""

dump_table() {
    local name="$1"
    local query="$2"
    local file="$DUMP_DIR/$name.csv"
    local done="$file.done"

    if [ -f "$done" ]; then
        echo "  $name: already done, skipping"
        return
    fi

    echo -n "  $name: dumping..."
    local start=$(date +%s)

    $PSQL "SET statement_timeout = 0; COPY ($query) TO '$file' WITH (FORMAT csv)"

    local end=$(date +%s)
    local elapsed=$((end - start))
    local size=$(du -h "$file" | cut -f1)
    echo " done ($size in ${elapsed}s)"

    echo "ok" > "$done"
}

# Small lookup tables (seconds)
dump_table "models" \
    'SELECT id, poi, type::text FROM "Model"'

dump_table "model_versions" \
    'SELECT id, "baseModel", "modelId" FROM "ModelVersion"'

dump_table "posts" \
    'SELECT id, extract(epoch from "publishedAt")::bigint, availability::text, "modelVersionId" FROM "Post"'

dump_table "tools" \
    'SELECT "toolId", "imageId" FROM "ImageTool"'

dump_table "techniques" \
    'SELECT "techniqueId", "imageId" FROM "ImageTechnique"'

# Medium tables
dump_table "resources" \
    'SELECT "imageId", "modelVersionId", detected FROM "ImageResourceNew"'

# Large tables (minutes)
dump_table "images" \
    'SELECT id, url, "nsfwLevel", hash, flags, type::text, "userId", "blockedFor", extract(epoch from "scannedAt")::bigint, extract(epoch from "createdAt")::bigint, "postId" FROM "Image"'

dump_table "tags" \
    'SELECT "tagId", "imageId" FROM "TagsOnImageDetails" WHERE disabled = false'

echo ""
echo "=== All dumps complete ==="
ls -lh "$DUMP_DIR"
