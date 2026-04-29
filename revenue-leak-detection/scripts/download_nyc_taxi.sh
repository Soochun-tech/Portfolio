#!/usr/bin/env bash

set -euo pipefail

YEAR="${YEAR:-2024}"
MONTHS=("01" "02" "03")
DEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/data/raw/nyc_taxi"
BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

mkdir -p "$DEST_DIR"

echo "Downloading NYC Yellow Cab trip data for ${YEAR} months: ${MONTHS[*]}"
echo "Destination: $DEST_DIR"
echo ""

for m in "${MONTHS[@]}"; do
  fname="yellow_tripdata_${YEAR}-${m}.parquet"
  url="${BASE_URL}/${fname}"
  dest="${DEST_DIR}/${fname}"

  if [[ -f "$dest" ]]; then
    size_mb=$(( $(stat -f%z "$dest" 2>/dev/null || stat -c%s "$dest") / 1024 / 1024 ))
    echo "  [skip] ${fname} already exists (${size_mb} MB)"
    continue
  fi

  echo "  [get ] ${fname}"
  curl -fL --progress-bar -o "$dest" "$url"
done

echo ""
echo "Done. Files in ${DEST_DIR}:"
ls -lh "$DEST_DIR"/*.parquet 2>/dev/null || echo "  (no parquet files found)"
