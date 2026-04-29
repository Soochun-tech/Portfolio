#!/usr/bin/env bash

set -euo pipefail

DEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/data/raw/nyc_taxi"
URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
DEST="${DEST_DIR}/taxi_zone_lookup.csv"

mkdir -p "$DEST_DIR"

if [[ -f "$DEST" ]]; then
  echo "[skip] taxi_zone_lookup.csv already exists"
else
  echo "[get ] taxi_zone_lookup.csv"
  curl -fL --progress-bar -o "$DEST" "$URL"
fi

echo "Done. $(wc -l < "$DEST") rows."
