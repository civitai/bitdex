#!/usr/bin/env bash
# Transfer dumped CSV files from the PG pod to the BitDex PVC.
#
# Usage:
#   bash tools/transfer-to-bitdex.sh [method]
#
# Methods:
#   kubectl-cp   - Use kubectl cp (default, works anywhere, slow for large files)
#   shared-node  - Direct copy via a hostPath mount (fast, requires same node)
#   custom       - Print the file list and let the user handle transfer
#
# Prerequisites:
#   - PG dump files exist at /var/lib/postgresql/data/bitdex_dump/ on cnpg-cluster-nvme0-1
#   - BitDex server scaled to 0 (for memory)
#   - Run pg-dump-tables.sh first

set -euo pipefail

PG_POD="cnpg-cluster-nvme0-1"
PG_NS="cnpg-database"
PG_DUMP_DIR="/var/lib/postgresql/data/bitdex_dump"

BITDEX_NS="bitdex"
BITDEX_STAGE_DIR="/data/indexes/civitai/load_stage"

METHOD="${1:-kubectl-cp}"

FILES=(
    models.csv
    model_versions.csv
    posts.csv
    tools.csv
    techniques.csv
    resources.csv
    images.csv
    tags.csv
)

echo "=== BitDex File Transfer ==="
echo "Method: $METHOD"
echo "Source: $PG_NS/$PG_POD:$PG_DUMP_DIR"
echo "Target: $BITDEX_NS PVC at $BITDEX_STAGE_DIR"
echo ""

# Ensure server is down
echo "Scaling BitDex server to 0..."
kubectl scale statefulset/bitdex -n "$BITDEX_NS" --replicas=0 2>/dev/null || true
kubectl wait --for=delete pod/bitdex-0 -n "$BITDEX_NS" --timeout=120s 2>/dev/null || true

case "$METHOD" in
    kubectl-cp)
        echo "Using kubectl cp (streams through API server)"
        echo ""

        # Create a transfer pod
        echo "Creating transfer pod..."
        kubectl run bitdex-transfer -n "$BITDEX_NS" --image=busybox \
            --overrides='{
                "spec": {
                    "containers": [{"name":"transfer","image":"busybox","command":["sleep","7200"],
                        "volumeMounts":[{"name":"data","mountPath":"/data"}]}],
                    "volumes": [{"name":"data","persistentVolumeClaim":{"claimName":"data-bitdex-0"}}],
                    "nodeSelector": {"kubernetes.io/hostname":"talos-fq9-f3k"}
                }
            }' --restart=Never 2>/dev/null || true
        kubectl wait --for=condition=ready pod/bitdex-transfer -n "$BITDEX_NS" --timeout=120s

        # Create staging directory
        kubectl exec -n "$BITDEX_NS" bitdex-transfer -- mkdir -p "$BITDEX_STAGE_DIR"

        # Transfer each file
        for f in "${FILES[@]}"; do
            DONE_MARKER="$BITDEX_STAGE_DIR/$f.done"

            # Check if already transferred
            if kubectl exec -n "$BITDEX_NS" bitdex-transfer -- test -f "$DONE_MARKER" 2>/dev/null; then
                echo "  $f: already transferred, skipping"
                continue
            fi

            echo -n "  $f: copying..."
            start=$(date +%s)

            kubectl cp "$PG_NS/$PG_POD:$PG_DUMP_DIR/$f" "$BITDEX_NS/bitdex-transfer:$BITDEX_STAGE_DIR/$f"

            end=$(date +%s)
            elapsed=$((end - start))
            echo " done (${elapsed}s)"

            # Write done marker
            kubectl exec -n "$BITDEX_NS" bitdex-transfer -- sh -c "echo ok > $DONE_MARKER"
        done

        echo ""
        echo "Verifying files..."
        kubectl exec -n "$BITDEX_NS" bitdex-transfer -- ls -lh "$BITDEX_STAGE_DIR"

        echo ""
        echo "Cleaning up transfer pod..."
        kubectl delete pod -n "$BITDEX_NS" bitdex-transfer
        ;;

    shared-node)
        echo "Using shared hostPath mount (both pods on same node)"
        echo ""
        echo "TODO: This requires knowing the hostPath for both PVCs."
        echo "If both pods are on the same node, you can mount both PVCs"
        echo "in a single pod and do a local cp."
        echo ""
        echo "Example: create a pod with both PVCs mounted, then:"
        echo "  cp /pg-data/bitdex_dump/*.csv /bitdex-data/indexes/civitai/load_stage/"
        echo "  for f in /bitdex-data/indexes/civitai/load_stage/*.csv; do echo ok > \$f.done; done"
        ;;

    custom)
        echo "Files to transfer from $PG_NS/$PG_POD:$PG_DUMP_DIR/:"
        echo ""
        for f in "${FILES[@]}"; do
            echo "  $f"
        done
        echo ""
        echo "Target: $BITDEX_NS PVC at $BITDEX_STAGE_DIR/"
        echo "Create .done markers after each file: echo ok > <file>.done"
        ;;

    *)
        echo "Unknown method: $METHOD"
        echo "Usage: $0 [kubectl-cp|shared-node|custom]"
        exit 1
        ;;
esac

echo ""
echo "=== Transfer complete ==="
echo ""
echo "Next steps:"
echo "  1. Run the bulk load job:"
echo "     kubectl create job -n bitdex --from=cronjob/bitdex-bulk-load bitdex-bulk-load-final"
echo "  2. Monitor: kubectl logs -n bitdex job/bitdex-bulk-load-final -f"
echo "  3. After completion, scale server back up:"
echo "     kubectl scale statefulset/bitdex -n bitdex --replicas=1"
echo "  4. Clean up PG dump files:"
echo "     MSYS_NO_PATHCONV=1 kubectl exec -n $PG_NS $PG_POD -- rm -rf $PG_DUMP_DIR"
