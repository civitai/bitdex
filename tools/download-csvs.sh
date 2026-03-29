#!/usr/bin/env bash
# Download gzipped CSVs from BitDex PVC to local data/load_stage/
# Usage: bash tools/download-csvs.sh [table1 table2 ...]
# If no tables specified, downloads all 9.

set -euo pipefail

CONTEXT="civit-datapacket"
NS="bitdex"
POD="bitdex-0"
CONTAINER="bitdex"
REMOTE_DIR="/data/indexes/civitai/load_stage"
LOCAL_DIR="data/load_stage"

TABLES=(model_versions models tools techniques posts resources collections images tags)

# If args provided, use those instead
if [ $# -gt 0 ]; then
  TABLES=("$@")
fi

mkdir -p "$LOCAL_DIR"

for table in "${TABLES[@]}"; do
  file="${table}.csv.gz"
  echo "Downloading ${file}..."
  MSYS_NO_PATHCONV=1 kubectl exec -n "$NS" "$POD" -c "$CONTAINER" --context "$CONTEXT" \
    -- cat "${REMOTE_DIR}/${file}" > "${LOCAL_DIR}/${file}"
  size=$(ls -lh "${LOCAL_DIR}/${file}" | awk '{print $5}')
  echo "  ${file}: ${size}"
done

echo ""
echo "All downloads complete:"
ls -lh "$LOCAL_DIR/"
