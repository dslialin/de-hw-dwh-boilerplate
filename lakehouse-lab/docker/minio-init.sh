#!/bin/bash
set -eu

# Wait until MinIO is reachable
echo "Waiting for MinIO at http://minio:9000 ..."
for i in $(seq 1 60); do
  if mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; then
    echo "MinIO is up."
    break
  fi
  sleep 1
done

# If alias still not set, fail loudly
mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

# Create bucket
mc mb -p local/lake || true
mc ls local
