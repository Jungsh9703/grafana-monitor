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

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] - [%(levelname)s] %(message)s in %(filename)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("/grafana_python/instance_principal_v/logs/filesystem_list.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("filesystem_list")

# DB 접속 정보 불러오기
load_dotenv()

# MySQL 접속 정보
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# ==============================
# OCI 클라이언트 (Instance Principal)
# ==============================
try:
    signer = InstancePrincipalsSecurityTokenSigner()
    region = os.getenv("OCI_REGION") or "ap-seoul-1"
    config = {"region": region}
    tenancy_id = os.getenv("TENANCY_OCID")

    fs_client = oci.file_storage.FileStorageClient(config, signer=signer)
    identity_client = oci.identity.IdentityClient(config, signer=signer)

    # 🔹 테넌시 이름 (표시용 루트 이름)
    tenancy = identity_client.get_tenancy(tenancy_id).data
    TENANCY_NAME = tenancy.name    # 필요하면 "gtopn"으로 고정해도 됨
    logger.info("API 연결 완료")
except Exception as e:
    logger.error(f"API 연결 중 오류 발생: {e}")
    raise

# ==============================
# 공용 함수
# ==============================
def as_naive_utc(dt):
    """tz-aware datetime → UTC naive."""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt

def build_2level_compartment_path(comp_id, comp_map, tenancy_id, tenancy_name, sep=" > "):
    """
    - 루트: gtopn
    - 루트 직속: gtopn > cloudteam
    - 그 아래: cloudteam > cwchoi
    """
    comp = comp_map.get(comp_id)
    if comp is None:
        return None

    name = comp.name
    parent_id = getattr(comp, "compartment_id", None)

    # 1) comp 자체가 루트거나 부모가 없는 경우 → 루트 이름만
    if comp_id == tenancy_id or parent_id is None:
        return tenancy_name

    # 2) 부모가 루트인 경우: gtopn > cloudteam
    if parent_id == tenancy_id:
        return f"{tenancy_name}{sep}{name}"

    # 3) 그 외에는 parent_name > child_name
    parent = comp_map.get(parent_id)
    if parent is None:
        return name

    return f"{parent.name}{sep}{name}"

# ==============================
# MySQL 연결 및 스키마 생성
# ==============================

try:
    MYSQL_CFG = {
        "host": DB_HOST,
        "user": DB_USER,
        "password": DB_PASS,
        "database": DB_NAME,
        "charset": "utf8mb4",
        "autocommit": True,
    }

    mysql_conn = pymysql.connect(**MYSQL_CFG)
    cur = mysql_conn.cursor()
    logger.info("MySQL 연결 완료")
except Exception as e:
    logger.error(f"MySQL 연결 중 오류 발생: {e}")
    raise

try:
    # 실행 시작 시각 (정리 기준용, DB 현재 UTC 시각)
    cur.execute("SELECT UTC_TIMESTAMP()")
    run_ts = cur.fetchone()[0]

    # ---------- FileSystem 테이블 ----------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS oci_fss_filesystems (
      fs_id               VARCHAR(200) PRIMARY KEY,
      display_name        VARCHAR(255),
      compartment_id      VARCHAR(200),
      compartment_name    VARCHAR(255),
      compartment_path    VARCHAR(500),
      availability_domain VARCHAR(100),
      lifecycle_state     VARCHAR(50),
      time_created        DATETIME,
      metered_bytes       BIGINT NULL,
      latest_snapshot_id    VARCHAR(200) NULL,
      latest_snapshot_name  VARCHAR(255) NULL,
      latest_snapshot_state VARCHAR(50)  NULL,
      latest_snapshot_time  DATETIME     NULL,
      last_seen_at        DATETIME NOT NULL,
      KEY idx_comp_ad (compartment_name, availability_domain),
      KEY idx_comp_path (compartment_path)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # ---------- Snapshot 테이블 ----------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS oci_fss_snapshots (
      snapshot_id     VARCHAR(200) PRIMARY KEY,
      fs_id           VARCHAR(200) NOT NULL,
      name            VARCHAR(255),
      lifecycle_state VARCHAR(50),
      time_created    DATETIME,
      time_ended      DATETIME NULL,
      deleted_at      DATETIME NULL,
      last_seen_at    DATETIME NOT NULL,
      KEY idx_fs_time (fs_id, time_created),
      KEY idx_state (lifecycle_state)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # ---------- Mount Target 테이블 ----------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS oci_fss_mount_targets (
      mt_id               VARCHAR(200) PRIMARY KEY,
      display_name        VARCHAR(255),
      compartment_id      VARCHAR(200),
      compartment_path    VARCHAR(500),
      availability_domain VARCHAR(100),
      lifecycle_state     VARCHAR(50),
      time_created        DATETIME,
      subnet_id           VARCHAR(200),
      export_set_id       VARCHAR(200),
      last_seen_at        DATETIME NOT NULL,
      KEY idx_mt_comp_path (compartment_path),
      KEY idx_mt_ad (availability_domain)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # ---------- Export 테이블 ----------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS oci_fss_exports (
      export_id      VARCHAR(200) PRIMARY KEY,
      fs_id          VARCHAR(200) NOT NULL,
      mt_id          VARCHAR(200) NOT NULL,
      path           VARCHAR(255),
      lifecycle_state VARCHAR(50),
      time_created   DATETIME,
      last_seen_at   DATETIME NOT NULL,
      KEY idx_ex_fs (fs_id),
      KEY idx_ex_mt (mt_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # ---------- UPSERT SQL ----------
    UPSERT_FS_SQL = """
    INSERT INTO oci_fss_filesystems (
      fs_id, display_name,
      compartment_id, compartment_name, compartment_path,
      availability_domain,
      lifecycle_state, time_created, metered_bytes,
      latest_snapshot_id, latest_snapshot_name, latest_snapshot_state, latest_snapshot_time,
      last_seen_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      display_name         = VALUES(display_name),
      compartment_id       = VALUES(compartment_id),
      compartment_name     = VALUES(compartment_name),
      compartment_path     = VALUES(compartment_path),
      availability_domain  = VALUES(availability_domain),
      lifecycle_state      = VALUES(lifecycle_state),
      time_created         = VALUES(time_created),
      metered_bytes        = VALUES(metered_bytes),
      latest_snapshot_id   = VALUES(latest_snapshot_id),
      latest_snapshot_name = VALUES(latest_snapshot_name),
      latest_snapshot_state= VALUES(latest_snapshot_state),
      latest_snapshot_time = VALUES(latest_snapshot_time),
      last_seen_at         = VALUES(last_seen_at);
    """

    UPSERT_SNAP_SQL = """
    INSERT INTO oci_fss_snapshots (
      snapshot_id, fs_id, name, lifecycle_state, time_created, time_ended, last_seen_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      name            = VALUES(name),
      lifecycle_state = VALUES(lifecycle_state),
      time_created    = VALUES(time_created),
      time_ended      = VALUES(time_ended),
      last_seen_at    = VALUES(last_seen_at);
    """

    UPSERT_MT_SQL = """
    INSERT INTO oci_fss_mount_targets (
      mt_id, display_name,
      compartment_id, compartment_path,
      availability_domain,
      lifecycle_state, time_created,
      subnet_id, export_set_id,
      last_seen_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      display_name       = VALUES(display_name),
      compartment_id     = VALUES(compartment_id),
      compartment_path   = VALUES(compartment_path),
      availability_domain= VALUES(availability_domain),
      lifecycle_state    = VALUES(lifecycle_state),
      time_created       = VALUES(time_created),
      subnet_id          = VALUES(subnet_id),
      export_set_id      = VALUES(export_set_id),
      last_seen_at       = VALUES(last_seen_at);
    """

    UPSERT_EXPORT_SQL = """
    INSERT INTO oci_fss_exports (
      export_id, fs_id, mt_id, path, lifecycle_state, time_created, last_seen_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      fs_id          = VALUES(fs_id),
      mt_id          = VALUES(mt_id),
      path           = VALUES(path),
      lifecycle_state= VALUES(lifecycle_state),
      time_created   = VALUES(time_created),
      last_seen_at   = VALUES(last_seen_at);
    """
    logger.info("테이블 초기 셋팅 완료")
except Exception as e:
    logger.error(f"테이블 초기 셋팅 중 오류 발생: {e}")
    raise

# ==============================
# 리스트 함수들
# ==============================
def list_all_compartments(tenancy_id):
    """tenancy 아래 모든 ACTIVE 컴파트먼트"""
    comps = []
    resp = oci.pagination.list_call_get_all_results(
        identity_client.list_compartments,
        compartment_id=tenancy_id,
        compartment_id_in_subtree=True
    )
    for c in resp.data:
        if c.lifecycle_state == "ACTIVE":
            comps.append(c)
    return comps


def list_availability_domains():
    return identity_client.list_availability_domains(tenancy_id).data


# ==============================
# 컴파트먼트 맵 & 2단계 경로 빌더
# ==============================
try:
    compartments = list_all_compartments(tenancy_id)
    logger.info("list_all_compartments() 완료")
except Exception as e:
    logger.error(f"list_all_compartments() 중 오류 발생: {e}")
    raise

comp_by_id = {c.id: c for c in compartments}

def build_compartment_path(comp_id: str) -> str:
    """
    - 루트: TENANCY_NAME (예: gtopn)
    - 루트 직속: TENANCY_NAME > cloudteam
    - 그 아래: cloudteam > cwchoi
    """
    return build_2level_compartment_path(
        comp_id,
        comp_by_id,
        tenancy_id,
        TENANCY_NAME,
        sep=" > "
    )

try:
    ads = list_availability_domains()
    logger.info("list_availability_domains() 완료")
except Exception as e:
    logger.error(f"list_availability_domains() 중 오류 발생: {e}")
    raise

# ==============================
# 이번 실행에서 실제로 본 리소스 ID 모음
# ==============================
seen_fs_ids = set()
seen_snapshot_ids = set()
seen_mt_ids = set()
seen_export_ids = set()

# ==============================
# 메인 루프: FS + Snapshot + MountTarget + Export 수집
# ==============================
logger.info("FS + Snapshot + MountTarget + Export 수집 시작")
for comp in compartments:
    try:
        comp_path = build_compartment_path(comp.id)
    except oci.exceptions.ServiceError as e:
        if e.status == 429:
            logger.warning(f"TooManyRequests: {comp.name} → 잠시 대기 후 재시도")
            time.sleep(3)
            continue
        else:
            logger.error(f"build_compartment_path() 중 오류 발생: {e}")
            raise

    for ad in ads:
        # ---------- 파일시스템 목록 ----------
        try:
            fs_list = oci.pagination.list_call_get_all_results(
                fs_client.list_file_systems,
                availability_domain=ad.name,
                compartment_id=comp.id
            ).data
        except oci.exceptions.ServiceError as e:
            if e.status == 429:
                logger.warning(f"TooManyRequests: {comp.name} → 잠시 대기 후 재시도")
                time.sleep(3)
                continue
            else:
                logger.error(f"파일시스템 목록 수집 중 오류 발생: {e}")
                raise

        # ---------- 마운트타겟 목록 ----------
        try:
            mt_list = oci.pagination.list_call_get_all_results(
                fs_client.list_mount_targets,
                availability_domain=ad.name,
                compartment_id=comp.id
            ).data
        except oci.exceptions.ServiceError as e:
            if e.status == 429:
                logger.warning(f"TooManyRequests: {comp.name} → 잠시 대기 후 재시도")
                time.sleep(3)
                continue
            else:
                logger.error(f"마운트타켓 목록 수집 중 오류 발생: {e}")
                raise

        if not fs_list and not mt_list:
            continue

        # ----- FileSystem + Snapshot -----
        for fs in fs_list:
            # 기본값: 스냅샷 없음
            snaps = []
            try:
                snaps_resp = oci.pagination.list_call_get_all_results(
                    fs_client.list_snapshots,
                    file_system_id=fs.id
                )
                snaps = snaps_resp.data
            except oci.exceptions.ServiceError as e:
                # 파일시스템이 이미 삭제됐거나 권한 문제로 404 나는 경우
                if e.status == 404:
                    logger.warning(
                        f"파일시스템 {fs.id} 스냅샷 조회 404(NotAuthorizedOrNotFound) → 스냅샷 없음으로 처리"
                    )
                    snaps = []
                # TooManyRequests → 잠깐 쉬고 스냅샷은 없는 것으로 간주
                elif e.status == 429:
                    logger.warning(
                        f"TooManyRequests: {comp.name} / FS {fs.id} 스냅샷 조회 → 잠시 대기 후 스냅샷 없이 진행"
                    )
                    time.sleep(3)
                    snaps = []
                else:
                    logger.error(f"스냅샷 목록 수집 중 오류 발생: {e}")
                    raise

            latest = max(snaps, key=lambda s: s.time_created) if snaps else None

            try:
                # FS 정보 UPSERT
                cur.execute(UPSERT_FS_SQL, (
                    fs.id,
                    fs.display_name,
                    fs.compartment_id,
                    comp.name,
                    comp_path,
                    fs.availability_domain,
                    fs.lifecycle_state,
                    as_naive_utc(fs.time_created),
                    getattr(fs, "metered_bytes", None),
                    latest.id if latest else None,
                    latest.name if latest else None,
                    latest.lifecycle_state if latest else None,
                    as_naive_utc(latest.time_created) if latest else None,
                    run_ts,
                ))
                seen_fs_ids.add(fs.id)

                # 스냅샷 정보 UPSERT
                for s in snaps:
                    time_ended = getattr(s, "time_ended", None)
                    cur.execute(UPSERT_SNAP_SQL, (
                        s.id,
                        fs.id,
                        s.name,
                        s.lifecycle_state,
                        as_naive_utc(s.time_created),
                        as_naive_utc(time_ended),
                        run_ts,
                    ))
                    seen_snapshot_ids.add(s.id)

            except Exception as e:
                logger.error(f"파일스토리지, 스냅샷 저장 중 오류 발생: {e}")
                raise

        # ----- MountTarget + Exports -----
        for mt in mt_list:
            try:
                cur.execute(UPSERT_MT_SQL, (
                    mt.id,
                    mt.display_name,
                    mt.compartment_id,
                    comp_path,
                    mt.availability_domain,
                    mt.lifecycle_state,
                    as_naive_utc(mt.time_created),
                    getattr(mt, "subnet_id", None),
                    getattr(mt, "export_set_id", None),
                    run_ts,
                ))
                seen_mt_ids.add(mt.id)
            except Exception as e:
                logger.error(f"마운트 타겟 저장 중 오류 발생: {e}")
                raise

            # 이 마운트타겟의 Export 목록 (export_set_id 기준)
            if getattr(mt, "export_set_id", None):
                try:
                    ex_list = oci.pagination.list_call_get_all_results(
                        fs_client.list_exports,
                        compartment_id=comp.id,
                        export_set_id=mt.export_set_id
                    ).data
                except oci.exceptions.ServiceError as e:
                    if e.status == 429:
                        logger.warning(f"TooManyRequests: {comp.name} → 잠시 대기 후 재시도")
                        time.sleep(3)
                        continue
                    else:
                        logger.error(f"Export 목록 수집 중 오류 발생: {e}")
                        raise
            else:
                ex_list = []

            for ex in ex_list:
                try:
                    cur.execute(UPSERT_EXPORT_SQL, (
                        ex.id,
                        ex.file_system_id,
                        mt.id,
                        ex.path,
                        ex.lifecycle_state,
                        as_naive_utc(ex.time_created),
                        run_ts,
                    ))
                    seen_export_ids.add(ex.id)
                except Exception as e:
                    logger.error(f"Export 목록 저장 중 오류 발생: {e}")
                    raise
        time.sleep(0.2)

logger.info("FS + Snapshot + MountTarget + Export 저장 완료")

# ============================
# 실행 끝: 이번 실행에서 한 번도 보이지 않은 리소스 하드 삭제
# ============================
logger.info("더 이상 보이지 않는 리소스 정리 시작")
try:
    # ----- FileSystem 정리 -----
    if seen_fs_ids:
        placeholders = ",".join(["%s"] * len(seen_fs_ids))
        delete_sql = f"""
        DELETE FROM oci_fss_filesystems
        WHERE fs_id NOT IN ({placeholders})
        """
        cur.execute(delete_sql, list(seen_fs_ids))
        logger.info(f"삭제된 FileSystem row 수: {cur.rowcount}")
    else:
        # 이번 실행에서 FS가 하나도 안 나온 경우 → 전부 삭제
        cur.execute("DELETE FROM oci_fss_filesystems")
        logger.info(f"FS 전체 삭제, row 수: {cur.rowcount}")

    # ----- Snapshot 정리 -----
    if seen_snapshot_ids:
        placeholders = ",".join(["%s"] * len(seen_snapshot_ids))
        delete_sql = f"""
        DELETE FROM oci_fss_snapshots
        WHERE snapshot_id NOT IN ({placeholders})
        """
        cur.execute(delete_sql, list(seen_snapshot_ids))
        logger.info(f"삭제된 Snapshot row 수: {cur.rowcount}")
    else:
        # 이번 실행에서 스냅샷이 하나도 없으면 전부 삭제
        cur.execute("DELETE FROM oci_fss_snapshots")
        logger.info(f"Snapshot 전체 삭제, row 수: {cur.rowcount}")

    # ----- MountTarget 정리 -----
    if seen_mt_ids:
        placeholders = ",".join(["%s"] * len(seen_mt_ids))
        delete_sql = f"""
        DELETE FROM oci_fss_mount_targets
        WHERE mt_id NOT IN ({placeholders})
        """
        cur.execute(delete_sql, list(seen_mt_ids))
        logger.info(f"삭제된 MountTarget row 수: {cur.rowcount}")
    else:
        cur.execute("DELETE FROM oci_fss_mount_targets")
        logger.info(f"MountTarget 전체 삭제, row 수: {cur.rowcount}")

    # ----- Export 정리 -----
    if seen_export_ids:
        placeholders = ",".join(["%s"] * len(seen_export_ids))
        delete_sql = f"""
        DELETE FROM oci_fss_exports
        WHERE export_id NOT IN ({placeholders})
        """
        cur.execute(delete_sql, list(seen_export_ids))
        logger.info(f"삭제된 Export row 수: {cur.rowcount}")
    else:
        cur.execute("DELETE FROM oci_fss_exports")
        logger.info(f"Export 전체 삭제, row 수: {cur.rowcount}")

    cur.close()
    mysql_conn.close()
    logger.info("더 이상 보이지 않는 리소스 정리 완료")
except Exception as e:
    logger.error(f"더 이상 보이지 않는 리소스 정리 중 오류 발생: {e}")
    raise

