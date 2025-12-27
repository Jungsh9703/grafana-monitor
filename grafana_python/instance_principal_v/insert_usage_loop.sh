#!/bin/bash

# 사용법: ./insert_usage_loop.sh YYYY-MM-DD YYYY-MM-DD
if [ $# -ne 2 ]; then
  echo "Usage: $0 START_DATE END_DATE"
  echo "Example: $0 2025-07-17 2025-09-23"
  exit 1
fi

START_DATE=$1
END_DATE=$2

current=$START_DATE
while [ "$current" != "$(date -I -d "$END_DATE + 1 day")" ]; do
  echo "[INFO] Inserting usage for $current"
  python3 /grafana_python/instance_principal_v/insert_usage.py $current
  current=$(date -I -d "$current + 1 day")
done
