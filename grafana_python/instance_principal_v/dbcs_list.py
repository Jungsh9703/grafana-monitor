#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import oci
import time
import logging
import pymysql
from datetime import datetime, timezone
from types import SimpleNamespace
from oci.auth.signers import InstancePrincipalsSecurityTokenSigner
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] - [%(levelname)s] %(message)s in %(filename)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("/grafana_python/instance_principal_v/logs/dbcs_list.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("dbcs_list")

# DB 접속 정보 불러오기
load_dotenv()

# MySQL 접속 정보
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# ============================
# MySQL 연결
# ============================
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

# ============================
# 테이블 생성
# ============================
try:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dbcs_inventory (
      dbsystem_id          VARCHAR(200) PRIMARY KEY,
      tenancy_ocid         VARCHAR(128) NOT NULL,
      region               VARCHAR(64)  NOT NULL,

      compartment_ocid     VARCHAR(200) NOT NULL,
      compartment_name     VARCHAR(255) NOT NULL,
      compartment_path     VARCHAR(500) NULL,

      display_name         VARCHAR(255) NOT NULL,
      db_home_count        INT          NULL,
      db_name_list         TEXT         NULL,

      lifecycle_state      VARCHAR(64)  NOT NULL,
      shape                VARCHAR(128) NOT NULL,
      cpu_core_count       INT          NULL,
      storage_size_gb      INT          NULL,

      node_count           INT          NULL,
      license_model        VARCHAR(64)  NULL,

      time_created_utc     DATETIME     NULL,
      last_refreshed_utc   DATETIME     NOT NULL,

      KEY idx_compartment (compartment_ocid),
      KEY idx_state       (lifecycle_state),
      KEY idx_region      (region)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # ============================
    # UPSERT SQL
    # ============================
    UPSERT_SQL = """
    INSERT INTO dbcs_inventory (
      dbsystem_id, tenancy_ocid, region,
      compartment_ocid, compartment_name, compartment_path,
      display_name, db_home_count, db_name_list,
      lifecycle_state, shape, cpu_core_count, storage_size_gb,
      node_count, license_model,
      time_created_utc, last_refreshed_utc
    )
    VALUES (
      %(dbsystem_id)s, %(tenancy_ocid)s, %(region)s,
      %(compartment_ocid)s, %(compartment_name)s, %(compartment_path)s,
      %(display_name)s, %(db_home_count)s, %(db_name_list)s,
      %(lifecycle_state)s, %(shape)s, %(cpu_core_count)s, %(storage_size_gb)s,
      %(node_count)s, %(license_model)s,
      %(time_created_utc)s, %(last_refreshed_utc)s
    )
    ON DUPLICATE KEY UPDATE
      compartment_name   = VALUES(compartment_name),
      compartment_path   = VALUES(compartment_path),
      display_name       = VALUES(display_name),
      db_home_count      = VALUES(db_home_count),
      db_name_list       = VALUES(db_name_list),
      lifecycle_state    = VALUES(lifecycle_state),
      shape              = VALUES(shape),
      cpu_core_count     = VALUES(cpu_core_count),
      storage_size_gb    = VALUES(storage_size_gb),
      node_count         = VALUES(node_count),
      license_model      = VALUES(license_model),
      time_created_utc   = VALUES(time_created_utc),
      last_refreshed_utc = VALUES(last_refreshed_utc);
    """
    logger.info("테이블 초기 셋팅 완료")
except Exception as e:
    logger.error(f"테이블 초기 셋팅 중 오류 발생: {e}")
    raise


# ============================
# Helper
# ============================
def to_dt(dt_obj):
    if dt_obj is None:
        return None
    if dt_obj.tzinfo is not None:
        return dt_obj.astimezone(timezone.utc).replace(tzinfo=None)
    return dt_obj

# ============================
# OCI 클라이언트 (Instance Principal)
# ============================

try:
    signer = InstancePrincipalsSecurityTokenSigner()
    region = os.getenv("OCI_REGION") or "ap-seoul-1"
    config = {"region": region}
    tenancy_ocid = os.getenv("TENANCY_OCID")

    identity_client = oci.identity.IdentityClient(config, signer=signer)
    db_client = oci.database.DatabaseClient(config, signer=signer)

    # 🔹 테넌시 이름 (루트 컴파트먼트 표시용)
    tenancy = identity_client.get_tenancy(tenancy_ocid).data
    TENANCY_NAME = tenancy.name   # 필요하면 "gtopn"으로 하드코딩해도 됨
    logger.info("API 연결 완료")
except Exception as e:
    logger.error(f"API 연결 중 오류 발생: {e}")
    raise

# ============================
# 컴파트먼트 전체 + 2단계 Path
# ============================
def load_compartments_with_parents():
    """
    tenancy 전체 컴파트먼트 트리 조회
    반환: {compartment_id: compartment_object}
    """
    resp = oci.pagination.list_call_get_all_results(
        identity_client.list_compartments,
        tenancy_ocid,
        compartment_id_in_subtree=True,
        access_level="ACCESSIBLE",
        sort_by="NAME",
    )

    comp_map = {}
    for c in resp.data:
        comp_map[c.id] = c

    return comp_map


def build_compartment_paths(comp_map, separator=" > "):
    """
    규칙:
      - 루트(tenancy): TENANCY_NAME
      - 루트 직속:     TENANCY_NAME > cloudteam
      - 그 아래:       cloudteam > cwchoi
    """
    paths = {}

    for cid, c in comp_map.items():
        if cid == tenancy_ocid:
            # 루트 자체는 dbcs_inventory에 안 들어가니까 패스
            continue

        name = c.name
        parent_id = getattr(c, "compartment_id", None)

        # 부모가 없거나 루트를 가리키면 → TENANCY_NAME > child
        if parent_id is None or parent_id == tenancy_ocid:
            paths[cid] = f"{TENANCY_NAME}{separator}{name}"
        else:
            parent = comp_map.get(parent_id)
            if parent:
                paths[cid] = f"{parent.name}{separator}{name}"
            else:
                paths[cid] = name

    return paths

# ----------------------------------
# DBCS 조회
# ----------------------------------
def list_dbcs(compartment_id):
    return oci.pagination.list_call_get_all_results(
        db_client.list_db_systems,
        compartment_id=compartment_id
    ).data

# ----------------------------------
# UPSERT 처리
# ----------------------------------
def upsert_dbcs(dbs, comp_name, comp_id, comp_path, run_ts, seen_ids: set):
    for dbs_item in dbs:
        db_names = []

        # List DB Homes → DB Names
        homes = db_client.list_db_homes(
            compartment_id=comp_id,
            db_system_id=dbs_item.id
        ).data

        for home in homes:
            db_list = db_client.list_databases(
                compartment_id=comp_id,
                db_home_id=home.id
            ).data
            for db in db_list:
                if db.db_name:
                    db_names.append(db.db_name)

        row = {
            "dbsystem_id": dbs_item.id,
            "tenancy_ocid": tenancy_ocid,
            "region": region,

            "compartment_ocid": comp_id,
            "compartment_name": comp_name,
            "compartment_path": comp_path,

            "display_name": dbs_item.display_name,
            "db_home_count": len(db_names),
            "db_name_list": ",".join(db_names) if db_names else None,

            "lifecycle_state": dbs_item.lifecycle_state,
            "shape": dbs_item.shape,
            "cpu_core_count": getattr(dbs_item, "cpu_core_count", None),
            "storage_size_gb": getattr(dbs_item, "data_storage_size_in_gb", None),
            "node_count": getattr(dbs_item, "node_count", None),
            "license_model": getattr(dbs_item, "license_model", None),

            "time_created_utc": to_dt(dbs_item.time_created),
            "last_refreshed_utc": run_ts,
        }
        cur.execute(UPSERT_SQL, row)
        seen_ids.add(dbs_item.id)

# ----------------------------------
# Main
# ----------------------------------
def main():
    logger.info("DBCS 목록 수집 시작")

    # 이번 실행 기준 타임스탬프 (microsecond 제거해서 깔끔하게)
    run_ts = datetime.utcnow().replace(microsecond=0, tzinfo=None)

    try:
        comp_map = load_compartments_with_parents()
        logger.info("load_compartments_with_parents() 완료")
    except Exception as e:
        logger.error(f"load_compartments_with_parents() 처리 중 오류 발생: {e}")
        raise

    try:
        comp_paths = build_compartment_paths(comp_map)
        logger.info("build_compartment_paths() 완료")
    except Exception as e:
        logger.error(f"build_compartment_paths() 처리 중 오류 발생: {e}")
        raise

    compartments = [
        c for cid, c in comp_map.items()
        if cid != tenancy_ocid and getattr(c, "lifecycle_state", "ACTIVE") == "ACTIVE"
    ]

    logger.info(f"컴파트먼트별 DBCS 목록 수집, 저장 진행 중")

    # 👉 이번 실행에서 실제로 조회된 DB System ID 모음
    seen_dbsystem_ids = set()

    for comp in compartments:
        comp_id = comp.id
        comp_name = comp.name
        comp_path = comp_paths.get(comp_id)

        try:
            dbs_list = list_dbcs(comp_id)
        except oci.exceptions.ServiceError as e:
            if e.status == 429:
                logger.warning(f"TooManyRequests: {comp.name} → 대기 후 재시도")
                time.sleep(3)
                continue
            else:
                logger.error(f"list_dbcs() 처리 중 오류 발생: {e}")
                raise
        except Exception as e:
            logger.error(f"list_dbcs() 처리 중 오류 발생: {e}")
            raise

        if not dbs_list:
            continue

        try:
            upsert_dbcs(dbs_list, comp_name, comp_id, comp_path, run_ts, seen_dbsystem_ids)
        except oci.exceptions.ServiceError as e:
            if e.status == 429:
                logger.warning(f"TooManyRequests: {comp.name} → 대기 후 재시도")
                time.sleep(3)
                continue
            else:
                logger.error(f"upsert_dbcs() 처리 중 오류 발생: {e}")
                raise
        except Exception as e:
            logger.error(f"upsert_dbcs() 처리 중 오류 발생: {e}")
            raise

    # 🔥 이번 실행에서 한 번도 보이지 않은 DBCS는 삭제
    try:
        if seen_dbsystem_ids:
            placeholders = ",".join(["%s"] * len(seen_dbsystem_ids))
            delete_sql = f"""
            DELETE FROM dbcs_inventory
            WHERE tenancy_ocid = %s
              AND region = %s
              AND dbsystem_id NOT IN ({placeholders})
            """
            params = [tenancy_ocid, region, *seen_dbsystem_ids]
            cur.execute(delete_sql, params)
            logger.info(f"삭제된 DBCS 정리 완료, {cur.rowcount}개 행 삭제")
        else:
            # 이번 실행에서 DBCS가 하나도 조회되지 않으면, 해당 테넌시/리전 전체 삭제
            delete_sql = """
            DELETE FROM dbcs_inventory
            WHERE tenancy_ocid = %s
              AND region = %s
            """
            cur.execute(delete_sql, (tenancy_ocid, region))
            logger.info(f"이번 실행에서 DBCS가 조회되지 않아, {cur.rowcount}개 행 전체 삭제")
    except Exception as e:
        logger.error(f"삭제된 DBCS 정리 중 오류 발생: {e}")
        raise

    logger.info("테넌시 내에 있는 DBCS 목록 저장 완료")


if __name__ == "__main__":
    main()

