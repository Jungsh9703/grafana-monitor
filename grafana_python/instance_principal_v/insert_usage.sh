#!/bin/bash

# 날짜 입력 받기 (YYYY-MM-DD)
if [ $# -eq 1 ]; then
  TARGET_DATE=$1
else
  # 입력이 없으면 어제 날짜
  TARGET_DATE=$(date -d "yesterday" +"%Y-%m-%d")
fi

echo "[INFO] Inserting usage for $TARGET_DATE"

python3 /grafana_python/instance_principal_v/insert_usage.py $TARGET_DATE
