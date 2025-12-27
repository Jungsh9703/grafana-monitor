#!/bin/bash
set -e

BASE_DIR="/grafana_python/instance_principal_v"

# 안전한 .env 로딩
set -a
source "$BASE_DIR/.env"
set +a

NOW_MIN=$(date +%M)
NOW_HOUR=$(date +%H)

# -----------------------------
# 1분마다 실행되는 작업
# -----------------------------
python3 "$BASE_DIR/adb_list.py" || true

# -----------------------------
# 5분마다 실행되는 작업
# -----------------------------
if (( 10#$NOW_MIN % 5 == 0 )); then
    python3 "$BASE_DIR/instance_list.py" || true
    python3 "$BASE_DIR/instance_volume.py" || true
    python3 "$BASE_DIR/dbcs_backup.py" || true
    python3 "$BASE_DIR/dbcs_list.py" || true
    python3 "$BASE_DIR/filesystem_list.py" || true
    python3 "$BASE_DIR/lb_list.py" || true
    python3 "$BASE_DIR/adb_backup.py" || true
fi

# -----------------------------
# 매일 01:00에 실행되는 작업
# -----------------------------
if [[ "$NOW_HOUR" == "01" && "$NOW_MIN" == "00" ]]; then
    python3 "$BASE_DIR/insert_usage.py" || true
fi