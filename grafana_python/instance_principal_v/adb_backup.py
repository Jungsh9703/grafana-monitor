#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Fetch OCI ADB backups and store into MySQL.
# - 현재 OCI API에서 조회되는 백업만 남기고 나머지는 삭제(전체 동기화 모드)

import os
import oci
import logging
import pymysql
from datetime import datetime, timezone
from oci.auth.signers import InstancePrincipalsSecurityTokenSigner
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] - [%(levelname)s] %(message)s in %(filename)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("/grafana_python/instance_principal_v/logs/adb_backup.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("adb_backup")

# DB 접속 정보 불러오기
load_dotenv()

# ==================== CONFIG ====================
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# 모니터링할 ADB OCID 리스트 (.env에서 콤마로 구분)
adb_ocids_comma = os.getenv("ADB_OCIDS", "")
ADB_OCIDS = [x.strip() for x in adb_ocids_comma.split(",") if x.strip()]

# ==================== DB Helper ====================
def mysql_connection():
    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        autocommit=True,
    )
    cur = conn.cursor()
    return cur, conn

def ensure_table(cur):
    """adb_backup_status 테이블이 없으면 생성"""
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS adb_backup_status (
            backup_id   VARCHAR(200) NOT NULL,
            adb_name    VARCHAR(100) NOT NULL,
            adb_status  VARCHAR(20),
            backup_status VARCHAR(20),
            time_started DATETIME,
            time_ended   DATETIME,
            created_at   DATETIME NOT NULL,
            PRIMARY KEY (backup_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )

def save_backup_to_mysql(adb_name, adb_status, backup, cur):
    """백업 1개를 adb_backup_status에 UPSERT"""
    sql = """
    INSERT INTO adb_backup_status
        (backup_id, adb_name, adb_status, backup_status, time_started, time_ended, created_at)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        adb_status    = VALUES(adb_status),
        backup_status = VALUES(backup_status),
        time_started  = VALUES(time_started),
        time_ended    = VALUES(time_ended),
        created_at    = VALUES(created_at)
    """

    cur.execute(
        sql,
        (
            backup.id,
            adb_name,
            adb_status,
            backup.lifecycle_state,
            backup.time_started.strftime("%Y-%m-%d %H:%M:%S") if backup.time_started else None,
            backup.time_ended.strftime("%Y-%m-%d %H:%M:%S") if backup.time_ended else None,
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )

# ==================== OCI Helper ====================
def get_adb_info(client, adb_id):
    """ADB 상태와 이름 조회"""
    response = client.get_autonomous_database(adb_id)
    return response.data.lifecycle_state, response.data.db_name

def get_all_backups(client, adb_id):
    """ADB의 전체 백업 목록 리턴"""
    backups = client.list_autonomous_database_backups(
        autonomous_database_id=adb_id
    ).data
    return backups

# ==================== MAIN ====================
if __name__ == "__main__":
    # ADB OCID 없으면 바로 종료
    if not ADB_OCIDS:
        logger.warning("ADB_OCIDS 환경변수가 비어있습니다. 처리할 ADB가 없습니다.")
        raise SystemExit(0)

    # ==== Instance Principal ====
    try:
        signer = InstancePrincipalsSecurityTokenSigner()
        region = os.getenv("OCI_REGION") or "ap-seoul-1"
        tenancy = os.getenv("TENANCY_OCID")
        config = {"region": region}

        db_client = oci.database.DatabaseClient(config, signer=signer)

        # API 연결 테스트
        identity = oci.identity.IdentityClient(config, signer=signer)
        identity.get_tenancy(tenancy)
        logger.info("API 연결 완료")
    except Exception as e:
        logger.error(f"API 연결 중 오류 발생: {e}")
        raise

    # MySQL 연결 + 테이블 생성
    try:
        cur, conn = mysql_connection()
        logger.info("MySQL 연결 완료")
        ensure_table(cur)
        logger.info("테이블 초기 셋팅 완료")
    except Exception as e:
        logger.error(f"MySQL 초기 셋팅 중 오류 발생: {e}")
        raise

    logger.info("ADB 백업 수집 시작 (전체 동기화 모드)")

    seen_backup_ids = set()  # 이번 실행에서 실제로 조회된 backup_id 집합
    total = 0

    for adb_id in ADB_OCIDS:
        try:
            adb_status, adb_name = get_adb_info(db_client, adb_id)
            logger.info(f"{adb_name} ({adb_id}) 백업 조회 시작")
        except Exception as e:
            logger.error(f"get_adb_info() 처리 중 오류 발생 (ADB: {adb_id}): {e}")
            continue

        try:
            backups = get_all_backups(db_client, adb_id)
            logger.info(f"get_all_backups() 완료 - {adb_name}, {len(backups)}개")
        except Exception as e:
            logger.error(f"get_all_backups() 처리 중 오류 발생 (ADB: {adb_name}): {e}")
            continue

        try:
            for b in backups:
                save_backup_to_mysql(adb_name, adb_status, b, cur)
                seen_backup_ids.add(b.id)
                total += 1
        except Exception as e:
            logger.error(f"save_backup_to_mysql() 처리 중 오류 발생 (ADB: {adb_name}): {e}")
            raise

        logger.info(f"{adb_name} 백업 저장 완료")

    logger.info(f"ADB 백업 수집 완료, 총 {total}개")

    # ========================
    # 🔥 전체 동기화 삭제 로직
    # ========================
    logger.info("adb_backup_status 테이블 동기화 시작 (조회되지 않은 백업 삭제)")

    try:
        if seen_backup_ids:
            placeholders = ",".join(["%s"] * len(seen_backup_ids))
            delete_sql = f"""
                DELETE FROM adb_backup_status
                WHERE backup_id NOT IN ({placeholders})
            """
            cur.execute(delete_sql, tuple(seen_backup_ids))
            logger.info(f"동기화로 삭제된 row 수: {cur.rowcount}")
        else:
            logger.info("이번 실행에서 조회된 백업이 없어 동기화 삭제 스킵")

        cur.close()
        conn.close()
        logger.info("adb_backup_status 테이블 동기화 완료")
    except Exception as e:
        logger.error(f"adb_backup_status 테이블 동기화 중 오류 발생: {e}")
        raise

