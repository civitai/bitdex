#!/bin/bash
# Analyze doc size distribution from DocStore V2 shard files.
# V2 shard format: 16-byte header, then tuples of [u32 slot][u16 field_idx][u16 value_len][value]
# This script samples N random shard files and computes per-slot total doc sizes.

DOCSTORE_DIR="${1:?Usage: $0 <docstore_dir> [sample_count]}"
SAMPLE_COUNT="${2:-100}"

if [ ! -d "$DOCSTORE_DIR" ]; then
    echo "ERROR: $DOCSTORE_DIR does not exist"
    exit 1
fi

echo "=== DocStore V2 Doc Size Distribution ==="
echo "Directory: $DOCSTORE_DIR"
echo "Sample size: $SAMPLE_COUNT shards"
echo ""

# Find all .bin shard files
SHARD_FILES=$(find "$DOCSTORE_DIR" -name "*.bin" -type f 2>/dev/null)
TOTAL_SHARDS=$(echo "$SHARD_FILES" | wc -l)
echo "Total shard files: $TOTAL_SHARDS"

# Total disk usage
DISK_USAGE=$(du -sh "$DOCSTORE_DIR" 2>/dev/null | awk '{print $1}')
echo "Total disk usage: $DISK_USAGE"
echo ""

# Sample random shards
echo "$SHARD_FILES" | shuf | head -n "$SAMPLE_COUNT" | while read -r f; do
    # Get file size (subtract 16 byte header = tuple data)
    FSIZE=$(stat -c %s "$f" 2>/dev/null || stat -f %z "$f" 2>/dev/null)
    if [ "$FSIZE" -gt 16 ]; then
        TUPLE_BYTES=$((FSIZE - 16))
        echo "$TUPLE_BYTES"
    fi
done | sort -n > /tmp/shard_sizes.txt

if [ ! -s /tmp/shard_sizes.txt ]; then
    echo "No valid shard files found"
    exit 1
fi

COUNT=$(wc -l < /tmp/shard_sizes.txt)
echo "Sampled $COUNT shards (tuple data bytes per shard):"
echo ""

# Statistics
awk '
BEGIN { sum=0; count=0 }
{
    vals[NR] = $1;
    sum += $1;
    count++;
}
END {
    mean = sum / count;
    p50_idx = int(count * 0.50) + 1;
    p95_idx = int(count * 0.95) + 1;
    p99_idx = int(count * 0.99) + 1;
    if (p50_idx > count) p50_idx = count;
    if (p95_idx > count) p95_idx = count;
    if (p99_idx > count) p99_idx = count;

    printf "  Count:  %d shards\n", count;
    printf "  Min:    %d bytes\n", vals[1];
    printf "  p50:    %d bytes\n", vals[p50_idx];
    printf "  Mean:   %.0f bytes\n", mean;
    printf "  p95:    %d bytes\n", vals[p95_idx];
    printf "  p99:    %d bytes\n", vals[p99_idx];
    printf "  Max:    %d bytes\n", vals[count];
    printf "  Total:  %.2f MB (sampled)\n", sum / 1048576;
}
' /tmp/shard_sizes.txt

echo ""
echo "Note: These are per-SHARD sizes (512 docs/shard with SHARD_SHIFT=9)."
echo "Divide by ~512 for approximate per-doc sizes."
