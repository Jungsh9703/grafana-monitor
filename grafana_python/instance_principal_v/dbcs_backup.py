#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Fetch OCI DB backups and store into MySQL with display_name/freeform_tags.
# - size_gb는 생성(STORED GENERATED) 컬럼이므로 절대 값 쓰지 않음 (size_bytes만 저장)
# - manual_flag 전면 제거
# - 동기화 모드: 이번 실행에서 조회한 백업만 남기고, 나머지는 삭제
import os
import oci
import json
import logging
import pymysql
from datetime import datetime, timezone, timedelta
from oci.auth.signers import InstancePrincipalsSecurityTokenSigner
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] - [%(levelname)s] %(message)s in %(filename)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("/grafana_python/instance_principal_v/logs/dbcs_backup.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("dbcs_backup")

# DB 접속 정보 불러오기
load_dotenv()

# ==================== CONFIG ====================
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# ✅ 테이블 이름은 고정: dbcs_backup_status
TABLE_NAME = "dbcs_backup_status"

# ✅ 모니터링할 DBCS OCID 목록은 .env 의 DBCS_OCIDS 에서 읽음
# 예) DBCS_OCIDS="ocid1.database....,ocid1.database...."
DBCS_OCIDS_RAW = os.getenv("DBCS_OCIDS", "") or ""
DB_OCIDS = [x.strip() for x in DBCS_OCIDS_RAW.split(",") if x.strip()]

if not DB_OCIDS:
    logger.warning("DBCS_OCIDS 환경변수가 비어있습니다. 처리할 데이터베이스가 없습니다.")
    # 백업이 하나도 없으면 의미가 없으니 조용히 종료
    exit(0)

LOOKBACK_DAYS = 14  # 최근 N일만 동기화. 전체 동기화라면 None

# ==================== HELPERS ====================
def connect_mysql():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

def ensure_table_and_columns(cur):
    # manual_flag 제거, size_gb는 생성컬럼(테이블에 이미 존재해도 이 코드는 건드리지 않음)
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id VARCHAR(200) PRIMARY KEY,
        db_name VARCHAR(100),
        type VARCHAR(20),
        time_started DATETIME NULL,
        time_ended DATETIME NULL,
        lifecycle_state VARCHAR(20) NOT NULL,
        size_bytes BIGINT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    cur.execute(f"""
        SELECT COLUMN_NAME FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
    """, (DB_NAME, TABLE_NAME))
    cols = {r["COLUMN_NAME"].lower() for r in cur.fetchall()}
    if "display_name" not in cols:
        cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN display_name VARCHAR(255) NULL")
    if "freeform_tags" not in cols:
        cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN freeform_tags JSON NULL")

def parse_size_fields(backup):
    # size_gb는 DB에서 생성되므로 여기선 size_bytes만 계산
    for attr in ("size_in_bytes", "size_in_gbs", "size_in_mbs", "size_in_gigabytes"):
        v = getattr(backup, attr, None)
        if v is None:
            continue
        try:
            if attr.endswith("bytes"):
                return int(v)
            if attr.endswith("gbs") or attr.endswith("gigabytes"):
                return int(float(v) * 1024**3)
            if attr.endswith("mbs"):
                return int(float(v) * 1024**2)
        except Exception:
            pass
    return None

def to_dt(ts):
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc).replace(tzinfo=None)
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return None

def upsert_backup(cur, row):
    # size_gb, manual_flag 전면 배제
    r = dict(row)
    r.pop("size_gb", None)

    cols = [
        "id", "db_name", "type", "time_started", "time_ended",
        "lifecycle_state", "size_bytes", "display_name", "freeform_tags"
    ]
    placeholders = [f"%({c})s" for c in cols]

    sql = f"""
    INSERT INTO {TABLE_NAME} ({", ".join(cols)})
    VALUES ({", ".join(placeholders)})
    ON DUPLICATE KEY UPDATE
      db_name=VALUES(db_name),
      type=VALUES(type),
      time_started=VALUES(time_started),
      time_ended=VALUES(time_ended),
      lifecycle_state=VALUES(lifecycle_state),
      size_bytes=VALUES(size_bytes),
      display_name=VALUES(display_name),
      freeform_tags=VALUES(freeform_tags)
    """
    cur.execute(sql, r)

# ----------------------------------
# Main
# ----------------------------------
def main():
    logger.info("DBCS 백업 수집 시작")

    try:
        conn = connect_mysql()
        cur = conn.cursor()
        logger.info("MySQL 연결 완료")
    except Exception as e:
        logger.error(f"MySQL 연결 중 오류 발생: {e}")
        raise

    try:
        ensure_table_and_columns(cur)
        logger.info("테이블 초기 셋팅 완료")
    except Exception as e:
        logger.error(f"테이블 초기 셋팅 중 오류 발생: {e}")
        raise

    try:
        signer = InstancePrincipalsSecurityTokenSigner()
        region = os.getenv("OCI_REGION") or "ap-seoul-1"
        config = {"region": region}
        tenancy_id = os.getenv("TENANCY_OCID")

        db_client = oci.database.DatabaseClient(config, signer=signer)
        identity_client = oci.identity.IdentityClient(config, signer=signer)
        tenancy = identity_client.get_tenancy(tenancy_id).data
        logger.info("API 연결 완료")
    except Exception as e:
        logger.error(f"API 연결 중 오류 발생: {e}")
        raise

    lookback_ts = None
    if LOOKBACK_DAYS:
        lookback_ts = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    total = 0
    seen_ids = set()

    logger.info("DBCS 백업 목록 수집, 저장 중")
    for db_ocid in DB_OCIDS:
        # 🔹 여기서 실제 DB 이름/표시이름을 OCI에서 조회
        try:
            db_info = db_client.get_database(db_ocid).data
            db_name = db_info.db_name or db_info.db_unique_name or db_info.display_name or db_ocid
        except Exception as e:
            logger.warning(f"DB 정보 조회 실패({db_ocid}) → OCID를 db_name으로 사용: {e}")
            db_name = db_ocid

        next_page = None

        while True:
            kwargs = dict(database_id=db_ocid, limit=1000)
            if next_page:
                kwargs["page"] = next_page
            try:
                resp = db_client.list_backups(**kwargs)
            except Exception as e:
                logger.error(f"DBCS 백업 목록 수집 중 오류 발생: {e}")
                raise

            backups = resp.data or []

            for b in backups:
                ts = getattr(b, "time_started", None)
                if lookback_ts and ts and ts < lookback_ts:
                    continue
                try:
                    size_bytes = parse_size_fields(b)
                except Exception as e:
                    logger.error(f"parse_size_fields() 처리 중 오류 발생: {e}")
                    raise

                row = {
                    "id": b.id,
                    "db_name": db_name,
                    "type": getattr(b, "type", None),
                    "time_started": to_dt(b.time_started),
                    "time_ended": to_dt(b.time_ended),
                    "lifecycle_state": getattr(b, "lifecycle_state", None),
                    "size_bytes": size_bytes,
                    "display_name": getattr(b, "display_name", None),
                    "freeform_tags": getattr(b, "freeform_tags", None)
                }

                try:
                    upsert_backup(cur, row)
                except Exception as e:
                    logger.error(f"upsert_backup() 처리 중 오류 발생: {e}")
                    raise

                seen_ids.add(row["id"])
                total += 1

            next_page = resp.headers.get("opc-next-page")
            if not next_page:
                break

    logger.info(f"DBCS 백업 수집 완료, 총 {total}개")

    # -------------------
    # 동기화 삭제
    # -------------------
    logger.info("테이블 동기화 작업 시작")
    try:
        if seen_ids:
            placeholders_id = ",".join(["%s"] * len(seen_ids))
            delete_sql = f"""
                DELETE FROM {TABLE_NAME}
                WHERE id NOT IN ({placeholders_id})
            """
            cur.execute(delete_sql, tuple(seen_ids))
            logger.info(f"동기화로 삭제된 row 수: {cur.rowcount}")
        else:
            logger.info("이번 실행에서 조회된 백업이 없어 동기화 삭제 스킵")

        cur.close()
        conn.close()
        logger.info("테이블 동기화 작업 완료")
    except Exception as e:
        logger.error(f"테이블 동기화 작업 중 오류 발생: {e}")
        raise


if __name__ == "__main__":
    main()

