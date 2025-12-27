#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import oci
import time
import logging
import pymysql
from datetime import datetime, timezone
from oci.auth.signers import InstancePrincipalsSecurityTokenSigner
from dotenv import load_dotenv

# ----------------------------------
# 1) 로깅 설정 (전역에서 단 한 번만)
# ----------------------------------
LOG_PATH = "/grafana_python/instance_principal_v/logs/adb_list.log"
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] - [%(levelname)s] %(message)s in %(filename)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("adb_list")

# DB 접속 정보 불러오기
load_dotenv()

# MySQL 접속 정보
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# ====================
# MySQL 연결 설정
# ====================
try:
    MYSQL = {
        "host": DB_HOST,
        "user": DB_USER,
        "password": DB_PASS,
        "database": DB_NAME,
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": True,
    }
    conn = pymysql.connect(**MYSQL)
    cur = conn.cursor()
    logger.info("MySQL 연결 완료")

except Exception as e:
    logger.error(f"MySQL 연결 중 오류 발생: {e}")
    raise

# ====================
# 테이블 생성 (compartment_path 포함)
# ====================
try:
    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS adb_inventory (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      tenancy_ocid        VARCHAR(128) NOT NULL,
      region              VARCHAR(64)  NOT NULL,
      compartment_ocid    VARCHAR(128) NOT NULL,
      compartment_name    VARCHAR(255) NOT NULL,
      compartment_path    VARCHAR(500) NULL,
      adb_ocid            VARCHAR(128) NOT NULL,
      display_name        VARCHAR(255) NOT NULL,
      db_name             VARCHAR(128) NOT NULL,
      workload            VARCHAR(32)  NOT NULL,
      lifecycle           VARCHAR(32)  NOT NULL,
      compute_type        VARCHAR(16)  NOT NULL,
      compute_count       INT          NULL,
      storage_gb          INT          NULL,
      auto_scaling        TINYINT(1)   NOT NULL,
      time_created_utc    DATETIME     NULL,
      last_refreshed_utc  DATETIME     NOT NULL,
      UNIQUE KEY uk_adb (adb_ocid),
      KEY idx_compartment (compartment_ocid),
      KEY idx_lifecycle (lifecycle),
      KEY idx_region (region)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    cur.execute(CREATE_TABLE_SQL)

    # ====================
    # UPSERT SQL (compartment_path 포함)
    # ====================
    UPSERT_SQL = """
    INSERT INTO adb_inventory (
      tenancy_ocid, region,
      compartment_ocid, compartment_name, compartment_path,
      adb_ocid, display_name, db_name,
      workload, lifecycle,
      compute_type, compute_count,
      storage_gb, auto_scaling,
      time_created_utc, last_refreshed_utc
    ) VALUES (
      %(tenancy_ocid)s, %(region)s,
      %(compartment_ocid)s, %(compartment_name)s, %(compartment_path)s,
      %(adb_ocid)s, %(display_name)s, %(db_name)s,
      %(workload)s, %(lifecycle)s,
      %(compute_type)s, %(compute_count)s,
      %(storage_gb)s, %(auto_scaling)s,
      %(time_created_utc)s, %(last_refreshed_utc)s
    )
    ON DUPLICATE KEY UPDATE
      tenancy_ocid       = VALUES(tenancy_ocid),
      region             = VALUES(region),
      compartment_ocid   = VALUES(compartment_ocid),
      compartment_name   = VALUES(compartment_name),
      compartment_path   = VALUES(compartment_path),
      display_name       = VALUES(display_name),
      db_name            = VALUES(db_name),
      workload           = VALUES(workload),
      lifecycle          = VALUES(lifecycle),
      compute_type       = VALUES(compute_type),
      compute_count      = VALUES(compute_count),
      storage_gb         = VALUES(storage_gb),
      auto_scaling       = VALUES(auto_scaling),
      time_created_utc   = VALUES(time_created_utc),
      last_refreshed_utc = VALUES(last_refreshed_utc);
    """
    logger.info("테이블 초기 셋팅 완료")

except Exception as e:
    logger.error(f"테이블 초기 셋팅 중 오류 발생: {e}")
    raise


def to_dt(dt_obj):
    """OCI datetime -> naive UTC datetime (MySQL DATETIME 호환)"""
    if dt_obj is None:
        return None
    if dt_obj.tzinfo is not None:
        return dt_obj.astimezone(timezone.utc).replace(tzinfo=None)
    return dt_obj

# ====================
# OCI 클라이언트 (Instance Principal)
# ====================
signer = InstancePrincipalsSecurityTokenSigner()
region = os.getenv("OCI_REGION") or "ap-seoul-1"
config = {"region": region}

tenancy_id = os.getenv("TENANCY_OCID")

try:
    database_client = oci.database.DatabaseClient(config, signer=signer)
    identity_client = oci.identity.IdentityClient(config, signer=signer)

    # 🔹 테넌시 이름
    tenancy = identity_client.get_tenancy(tenancy_id).data
    TENANCY_NAME = tenancy.name   # 필요하면 "gtopn"으로 고정 가능
    logger.info("API 연결 완료")
except Exception as e:
    logger.error(f"API 연결 중 오류 발생: {e}")
    raise

# ====================
# 컴파트먼트 조회 + 2단계 path 생성
# ====================
def list_all_compartments(tenancy_id: str):
    """ACTIVE 컴파트먼트 전체 조회"""
    compartments = []
    response = oci.pagination.list_call_get_all_results(
        identity_client.list_compartments,
        compartment_id=tenancy_id,
        compartment_id_in_subtree=True
    )
    for c in response.data:
        if c.lifecycle_state == "ACTIVE":
            compartments.append(c)
    return compartments


def build_compartment_paths(compartments, tenancy_id: str, separator=" > "):
    """
    각 compartment_id 에 대해 2단계 path 생성
    규칙:
      - 루트(tenancy): TENANCY_NAME
      - 루트 직속:     TENANCY_NAME > cloudteam
      - 그 아래:       cloudteam > cwchoi
    """
    comp_map = {c.id: c for c in compartments}
    paths = {}

    for cid, c in comp_map.items():
        name = c.name
        parent_id = getattr(c, "compartment_id", None)

        # 부모가 없거나 루트를 가리키면 → TENANCY_NAME > child
        if parent_id is None or parent_id == tenancy_id:
            paths[cid] = f"{TENANCY_NAME}{separator}{name}"
        else:
            parent = comp_map.get(parent_id)
            if parent:
                paths[cid] = f"{parent.name}{separator}{name}"
            else:
                paths[cid] = name

    return paths

# ====================
# ADB UPSERT
# ====================
def upsert_adb_row(adb, comp, comp_path: str, run_ts, seen_ids: set):
    # ECPU/OCPU 판별
    ecpu = getattr(adb, "compute_count", None)
    ocpu = getattr(adb, "cpu_core_count", None)
    if ecpu is not None:
        compute_type = "ECPU"
        compute_count = int(ecpu)
    elif ocpu is not None:
        compute_type = "OCPU"
        compute_count = int(ocpu)
    else:
        compute_type = "UNKNOWN"
        compute_count = None

    # 스토리지 GB
    storage_tbs = getattr(adb, "data_storage_size_in_tbs", None)
    storage_gb = int(storage_tbs * 1024) if storage_tbs is not None else None

    # 오토스케일
    auto_scaling = 1 if getattr(adb, "is_auto_scaling_enabled", False) else 0

    row = {
        "tenancy_ocid": tenancy_id,
        "region": region,
        "compartment_ocid": comp.id,
        "compartment_name": comp.name,
        "compartment_path": comp_path,  # ✅ 2단계 path
        "adb_ocid": adb.id,
        "display_name": adb.display_name or "",
        "db_name": adb.db_name or "",
        "workload": (adb.db_workload or "UNKNOWN"),
        "lifecycle": (adb.lifecycle_state or "UNKNOWN"),
        "compute_type": compute_type,
        "compute_count": compute_count,
        "storage_gb": storage_gb,
        "auto_scaling": auto_scaling,
        "time_created_utc": to_dt(getattr(adb, "time_created", None)),
        "last_refreshed_utc": run_ts,
    }
    cur.execute(UPSERT_SQL, row)
    seen_ids.add(adb.id)

# ----------------------------------
# 5) main()
# ----------------------------------
def main():
    logger.info("테넌시 내에 있는 ADB 목록 수집 시작")

    # 이번 실행 기준 타임스탬프 (microsecond 제거)
    run_ts = datetime.utcnow().replace(microsecond=0, tzinfo=None)

    try:
        compartments = list_all_compartments(tenancy_id)
        logger.info("list_all_compartments() 완료")
    except Exception as e:
        logger.error(f"list_all_compartments() 처리 중 오류 발생: {e}")
        raise

    try:
        comp_paths = build_compartment_paths(compartments, tenancy_id)
        logger.info("build_compartment_paths() 완료")
    except Exception as e:
        logger.error(f"build_compartment_paths() 처리 중 오류 발생: {e}")
        raise

    logger.info("컴파트먼트별 ADB 목록 수집, 저장 진행 중")

    # 👉 이번 실행에서 실제로 조회된 ADB OCID 모음
    seen_adb_ids = set()

    for comp in compartments:
        comp_path = comp_paths.get(comp.id)
        try:
            adbs = oci.pagination.list_call_get_all_results(
                database_client.list_autonomous_databases,
                compartment_id=comp.id
            ).data
            if not adbs:
                continue
        except oci.exceptions.ServiceError as e:
            if e.status == 429:
                logger.warning(f"TooManyRequests: {comp.name} → 대기 후 재시도")
                time.sleep(3)
                continue
            else:
                logger.error(f"컴파트먼트별 ADB 목록 수집 중 오류 발생: {e}")
                raise

        try:
            for adb in adbs:
                upsert_adb_row(adb, comp, comp_path, run_ts, seen_adb_ids)
            time.sleep(0.2)
        except oci.exceptions.ServiceError as e:
            if e.status == 429:
                logger.warning(f"TooManyRequests: {comp.name} → 대기 후 재시도")
                time.sleep(3)
                continue
            else:
                logger.error(f"컴파트먼트별 ADB 목록 저장 중 오류 발생: {e}")
                raise
        except Exception as e:
            logger.error(f"컴파트먼트별 ADB 목록 저장 중 오류 발생: {e}")
            raise

    # 🔥 이번 실행에서 한 번도 보이지 않은 ADB는 삭제
    try:
        if seen_adb_ids:
            placeholders = ",".join(["%s"] * len(seen_adb_ids))
            delete_sql = f"""
            DELETE FROM adb_inventory
            WHERE tenancy_ocid = %s
              AND region = %s
              AND adb_ocid NOT IN ({placeholders})
            """
            params = [tenancy_id, region, *seen_adb_ids]
            cur.execute(delete_sql, params)
            logger.info(f"삭제된 ADB 정리 완료, {cur.rowcount}개 행 삭제")
        else:
            # 이번 실행에서 ADB가 하나도 조회되지 않으면, 해당 테넌시/리전 전체 삭제
            delete_sql = """
            DELETE FROM adb_inventory
            WHERE tenancy_ocid = %s
              AND region = %s
            """
            cur.execute(delete_sql, (tenancy_id, region))
            logger.info(f"이번 실행에서 ADB가 조회되지 않아, {cur.rowcount}개 행 전체 삭제")
    except Exception as e:
        logger.error(f"삭제된 ADB 정리 중 오류 발생: {e}")
        raise

    logger.info("테넌시 내에 있는 ADB 목록 저장 완료")

# ----------------------------------
# 6) entrypoint
# ----------------------------------
if __name__ == "__main__":
    main()

