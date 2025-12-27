#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import oci
import pymysql
from datetime import datetime, timezone
import os
import logging
from oci.auth.signers import InstancePrincipalsSecurityTokenSigner
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] - [%(levelname)s] %(message)s in %(filename)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("/grafana_python/instance_principal_v/logs/instance_volume.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("instance_volume")

# DB 접속 정보 불러오기
load_dotenv()

# ===== MySQL =====
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# ===== 대상 인스턴스 OCID 목록 (.env에서 읽기) =====
# 예) INSTANCE_OCIDS="ocid1.instance....aaa,ocid1.instance....bbb"
INSTANCE_OCIDS_RAW = os.getenv("INSTANCE_OCIDS", "")
INSTANCE_OCIDS = [x.strip() for x in INSTANCE_OCIDS_RAW.split(",") if x.strip()]

if not INSTANCE_OCIDS:
    logger.warning("INSTANCE_OCIDS 환경변수가 비어있습니다. 처리할 인스턴스가 없습니다.")

# 풀 리프레시: 매 실행 시 전량 삭제 후 재삽입
FULL_REFRESH_VOLUMES = True
FULL_REFRESH_BACKUPS = True   # 백업도 같은 방식으로 싹 지우고 “현재 붙은 볼륨”의 백업만 다시 삽입

ATTACH_OK = {"ATTACHED", "ATTACHING"}


def utc_naive(dt):
    if not dt:
        return None
    if isinstance(dt, datetime):
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


# ===== OCI =====
def get_instance_info(compute, instance_id):
    d = compute.get_instance(instance_id).data
    return d.display_name, d.compartment_id, d.availability_domain


def list_block_attachments(compute, block, instance_id, compartment_id):
    atts = oci.pagination.list_call_get_all_results(
        compute.list_volume_attachments,
        compartment_id=compartment_id,
        instance_id=instance_id,
    ).data
    out = []
    for a in atts:
        v = block.get_volume(a.volume_id).data
        out.append(
            {
                "attachment_id": a.id,
                "attachment_state": a.lifecycle_state,
                "volume_id": v.id,
                "volume_name": v.display_name,
                "size_gbs": v.size_in_gbs,
                "attachment_type": getattr(a, "attachment_type", "BLOCK"),
                "device": getattr(a, "device", None),
                "attach_state": a.lifecycle_state,
                "volume_state": v.lifecycle_state,
                "volume_type": "block",
                "time_created": utc_naive(v.time_created),
                "volume_compartment_id": v.compartment_id,
                "is_boot": False,
            }
        )
    return out


def list_boot_attachments(compute, block, instance_id, compartment_id, ad):
    atts = oci.pagination.list_call_get_all_results(
        compute.list_boot_volume_attachments,
        availability_domain=ad,
        compartment_id=compartment_id,
        instance_id=instance_id,
    ).data
    out = []
    for a in atts:
        v = block.get_boot_volume(a.boot_volume_id).data
        out.append(
            {
                "attachment_id": a.id,
                "attachment_state": a.lifecycle_state,
                "volume_id": v.id,
                "volume_name": v.display_name,
                "size_gbs": v.size_in_gbs,
                "attachment_type": "BOOT",
                "device": None,
                "attach_state": a.lifecycle_state,
                "volume_state": v.lifecycle_state,
                "volume_type": "boot",
                "time_created": utc_naive(v.time_created),
                "volume_compartment_id": v.compartment_id,
                "is_boot": True,
            }
        )
    return out


def list_backups(block, search, volume_id, is_boot, compartment_id):
    backs = []
    try:
        if is_boot:
            backs = block.list_boot_volume_backups(
                compartment_id=compartment_id, boot_volume_id=volume_id
            ).data
        else:
            backs = block.list_volume_backups(
                compartment_id=compartment_id, volume_id=volume_id
            ).data
    except oci.exceptions.ServiceError:
        backs = []

    if not backs:
        from oci.resource_search.models import StructuredSearchDetails

        q = (
            f"query bootvolumebackup resources where bootVolumeId = '{volume_id}'"
            if is_boot
            else f"query volumebackup resources where volumeId = '{volume_id}'"
        )
        try:
            resp = search.search_resources(
                StructuredSearchDetails(query=q, matching_context_type="NONE")
            )
            items = getattr(resp.data, "items", []) or []
            for it in items:
                try:
                    b = (
                        block.get_boot_volume_backup(it.identifier).data
                        if is_boot
                        else block.get_volume_backup(it.identifier).data
                    )
                    backs.append(b)
                except oci.exceptions.ServiceError:
                    pass
        except oci.exceptions.ServiceError:
            backs = []

    norm = []
    for b in backs:
        norm.append(
            {
                "backup_id": b.id,
                "backup_name": getattr(b, "display_name", None) or "-",
                "backup_type": getattr(b, "type", None),
                "lifecycle_state": b.lifecycle_state,
                "size_gbs": getattr(b, "size_in_gbs", None),
                "time_created": utc_naive(getattr(b, "time_created", None)),
                "expiration_time": utc_naive(getattr(b, "expiration_time", None)),
            }
        )
    norm.sort(key=lambda x: x["time_created"] or datetime.min, reverse=True)
    return norm


# ===== MySQL DDL =====
def ensure_tables(cur):
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS instance_volume_latest (
      instance_id       VARCHAR(200) NOT NULL,
      volume_id         VARCHAR(200) NOT NULL,
      instance_name     VARCHAR(200) NOT NULL,
      volume_name       VARCHAR(200) NOT NULL,
      volume_type       VARCHAR(16)  NOT NULL,  -- boot/block
      size_gbs          BIGINT,
      attachment_type   VARCHAR(32),
      device            VARCHAR(128) NULL,
      lifecycle_state   VARCHAR(32),
      volume_state      VARCHAR(32),
      time_created      DATETIME NULL,
      updated_at        DATETIME NOT NULL,
      attachment_id     VARCHAR(200) NULL,
      attachment_state  VARCHAR(32) NULL,
      last_seen_attached_at DATETIME NULL,
      PRIMARY KEY (instance_id, volume_id),
      KEY idx_instname (instance_name),
      KEY idx_state    (lifecycle_state),
      KEY idx_attachid (attachment_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    )
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS volume_backup_latest (
      backup_id       VARCHAR(200) PRIMARY KEY,
      instance_id     VARCHAR(200) NOT NULL,
      volume_id       VARCHAR(200) NOT NULL,
      backup_name     VARCHAR(255) NOT NULL,
      backup_type     VARCHAR(32),
      lifecycle_state VARCHAR(32),
      size_gbs        BIGINT NULL,
      time_created    DATETIME NULL,
      expiration_time DATETIME NULL,
      updated_at      DATETIME NOT NULL,
      KEY idx_inst (instance_id),
      KEY idx_vol  (volume_id),
      KEY idx_state(lifecycle_state)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    )


def insert_attached_volume(conn, instance_name, instance_id, v):
    cur = conn.cursor()
    cur.execute(
        """
    INSERT INTO instance_volume_latest
      (instance_id, volume_id, instance_name, volume_name, volume_type,
       size_gbs, attachment_type, device, lifecycle_state, volume_state, time_created,
       updated_at, attachment_id, attachment_state, last_seen_attached_at)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(),%s,%s,UTC_TIMESTAMP())
    ON DUPLICATE KEY UPDATE
      instance_name=VALUES(instance_name),
      volume_name=VALUES(volume_name),
      volume_type=VALUES(volume_type),
      size_gbs=VALUES(size_gbs),
      attachment_type=VALUES(attachment_type),
      device=VALUES(device),
      lifecycle_state=VALUES(lifecycle_state),
      volume_state=VALUES(volume_state),
      time_created=VALUES(time_created),
      updated_at=UTC_TIMESTAMP(),
      attachment_id=VALUES(attachment_id),
      attachment_state=VALUES(attachment_state),
      last_seen_attached_at=UTC_TIMESTAMP()
    """,
        (
            instance_id,
            v["volume_id"],
            instance_name,
            v["volume_name"],
            v["volume_type"],
            v["size_gbs"],
            v["attachment_type"],
            v["device"],
            v["attach_state"],
            v["volume_state"],
            v["time_created"],
            v["attachment_id"],
            v["attachment_state"],
        ),
    )
    conn.commit()
    cur.close()


def delete_instance_rows(conn, table, instance_id):
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {table} WHERE instance_id=%s", (instance_id,))
    conn.commit()
    cur.close()


def upsert_backup_latest(conn, instance_id, volume_id, b):
    cur = conn.cursor()
    cur.execute(
        """
    INSERT INTO volume_backup_latest
      (backup_id, instance_id, volume_id, backup_name, backup_type,
       lifecycle_state, size_gbs, time_created, expiration_time, updated_at)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP())
    ON DUPLICATE KEY UPDATE
      instance_id=VALUES(instance_id),
      volume_id=VALUES(volume_id),
      backup_name=VALUES(backup_name),
      backup_type=VALUES(backup_type),
      lifecycle_state=VALUES(lifecycle_state),
      size_gbs=VALUES(size_gbs),
      time_created=VALUES(time_created),
      expiration_time=VALUES(expiration_time),
      updated_at=UTC_TIMESTAMP()
    """,
        (
            b["backup_id"],
            instance_id,
            volume_id,
            b["backup_name"],
            b["backup_type"],
            b["lifecycle_state"],
            b["size_gbs"],
            b["time_created"],
            b["expiration_time"],
        ),
    )
    conn.commit()
    cur.close()


# ===== 메인 =====
if __name__ == "__main__":
    # ==== Instance Principal ====
    try:
        signer = InstancePrincipalsSecurityTokenSigner()
        region = os.getenv("OCI_REGION")
        if not region:
            region = "ap-seoul-1"
        config = {"region": region}

        compute = oci.core.ComputeClient(config, signer=signer)
        block = oci.core.BlockstorageClient(config, signer=signer)
        search = oci.resource_search.ResourceSearchClient(config, signer=signer)
        logger.info("API 연결 완료")
    except Exception as e:
        logger.info(f"API 연결 중 오류 발생: {e}")
        raise

    try:
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            charset="utf8mb4",
            autocommit=True,
        )

        # ✅ 시작 시 한 번만 테이블 생성
        c = conn.cursor()
        logger.info("MySQL 연결 완료")
    except Exception as e:
        logger.error(f"MySQL 연결 중 오류 발생: {e}")
        raise
    try:
        ensure_tables(c)
        logger.info("테이블 초기 셋팅 완료")
    except Exception as e:
        logger.error(f"테이블 초기 셋팅 중 오류 발생: {e}")
        raise
    c.close()

    logger.info("인스턴스 볼륨 목록 수집 시작")

    for inst_id in INSTANCE_OCIDS:
        try:
            name, comp, ad = get_instance_info(compute, inst_id)
        except oci.exceptions.ServiceError as e:
            if getattr(e, "status", None) == 404:
                logger.warning(f"삭제된 인스턴스 의심: {e}")
                continue
            else:
                logger.error(f"get_instance_info() 중 오류 발생: {e}")
                raise
        try:
            boots = list_boot_attachments(compute, block, inst_id, comp, ad)
        except Exception as e:
            logger.error(f"list_boot_attachments() 중 오류 발생: {e}")
            raise

        try:
            blks = list_block_attachments(compute, block, inst_id, comp)
        except Exception as e:
            logger.error(f"list_block_attachments() 중 오류 발생: {e}")
            raise

        vols = boots + blks

        # === 풀 리프레시: 기존 행 삭제 ===
        if FULL_REFRESH_VOLUMES:
            try:
                delete_instance_rows(conn, "instance_volume_latest", inst_id)
            except Exception as e:
                logger.error(f"delete_instance_rows() 중 오류 발생: {e}")
                raise

        attached_now = [v for v in vols if v["attach_state"] in ATTACH_OK]
        for v in attached_now:
            try:
                insert_attached_volume(conn, name, inst_id, v)
            except Exception as e:
                logger.error(f"insert_attached_volume() 중 오류 발생: {e}")
                raise

        if FULL_REFRESH_BACKUPS:
            try:
                delete_instance_rows(conn, "volume_backup_latest", inst_id)
            except Exception as e:
                logger.error(f"delete_instance_rows() 중 오류 발생: {e}")
                raise

        for v in attached_now:
            try:
                backs = list_backups(
                    block, search, v["volume_id"], v["is_boot"], v["volume_compartment_id"]
                )
            except Exception as e:
                logger.error(f"list_backups() 중 오류 발생: {e}")
                raise
            for b in backs:
                try:
                    upsert_backup_latest(conn, inst_id, v["volume_id"], b)
                except Exception as e:
                    logger.error(f"upsert_backup_latest() 중 오류 발생: {e}")
                    raise

    conn.close()
    logger.info("인스턴스 볼륨 목록 저장 완료")

