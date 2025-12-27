#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import oci
import pymysql
from datetime import datetime, timezone
from types import SimpleNamespace
import os
import logging
from oci.auth.signers import InstancePrincipalsSecurityTokenSigner
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] - [%(levelname)s] %(message)s in %(filename)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("/grafana_python/instance_principal_v/logs/instance_list.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("instance_list")

# DB 접속 정보 불러오기
load_dotenv()

# MySQL 접속 정보
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")


# =========================
# MySQL 연결
# =========================
try:
    mysql_conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    cur = mysql_conn.cursor()
    logger.info("MySQL 연결 완료")
except Exception as e:
    logger.error(f"MySQL 연결 중 오류 발생: {e}")
    raise

# =========================
# OCI 설정 (Instance Principal)
# =========================
try:
    signer = InstancePrincipalsSecurityTokenSigner()
    region = os.getenv("OCI_REGION") or "ap-seoul-1"
    config = {"region": region}
    tenancy_ocid = os.getenv("TENANCY_OCID")

    compute_client = oci.core.ComputeClient(config, signer=signer)
    network_client = oci.core.VirtualNetworkClient(config, signer=signer)
    identity_client = oci.identity.IdentityClient(config, signer=signer)
    logger.info("API 연결 완료")
except Exception as e:
    logger.error(f"API 연결 중 오류 발생: {e}")
    raise

# =========================
# 테이블 자동 생성
# =========================
def init_schema(cur):
    ddl = """
    CREATE TABLE IF NOT EXISTS oci_instance_inventory (
      instance_id         VARCHAR(200) NOT NULL,
      tenancy_ocid        VARCHAR(128) NOT NULL,
      region              VARCHAR(64)  NOT NULL,

      compartment_id      VARCHAR(200) NOT NULL,
      compartment_name    VARCHAR(255) NULL,
      compartment_path    VARCHAR(500) NULL,

      display_name        VARCHAR(255) NOT NULL,
      shape               VARCHAR(128) NOT NULL,
      ocpus               DECIMAL(8,2) NULL,
      memory_gbs          DECIMAL(8,2) NULL,

      lifecycle_state     VARCHAR(32) NOT NULL,
      availability_domain VARCHAR(64) NULL,

      primary_vnic_id     VARCHAR(200) NULL,
      private_ips         TEXT NULL,
      public_ips          TEXT NULL,

      time_created_utc    DATETIME NULL,
      last_refreshed_utc  DATETIME NOT NULL,

      PRIMARY KEY (instance_id),
      KEY idx_inst_compartment (compartment_id),
      KEY idx_inst_region      (region),
      KEY idx_inst_state       (lifecycle_state),
      KEY idx_inst_display     (display_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    cur.execute(ddl)


# =========================
# 컴파트먼트 전체 로드 (+ tenancy root 포함)
# =========================
def load_compartments_with_root():
    tenancy = identity_client.get_tenancy(tenancy_ocid).data

    root = SimpleNamespace(
        id=tenancy_ocid,
        name=tenancy.name,
        compartment_id=None,
        lifecycle_state="ACTIVE",
    )

    resp = oci.pagination.list_call_get_all_results(
        identity_client.list_compartments,
        tenancy_ocid,
        compartment_id_in_subtree=True,
        access_level="ACCESSIBLE",
        sort_by="NAME",
    )

    comp_map = {tenancy_ocid: root}
    for c in resp.data:
        comp_map[c.id] = c

    return comp_map


# =========================
# 컴파트먼트 경로 생성 (2단계)
# =========================
def build_compartment_paths(comp_map, separator=" > "):
    paths = {}

    root = comp_map.get(tenancy_ocid)
    tenancy_name = root.name if root else "root"

    for cid, c in comp_map.items():
        if cid == tenancy_ocid:  # root
            paths[cid] = tenancy_name
            continue

        name = c.name
        parent_id = getattr(c, "compartment_id", None)

        # gtopn > cloudteam
        if parent_id is None or parent_id == tenancy_ocid:
            paths[cid] = f"{tenancy_name}{separator}{name}"
        else:
            # cloudteam > cwchoi
            parent = comp_map.get(parent_id)
            if parent:
                paths[cid] = f"{parent.name}{separator}{name}"
            else:
                paths[cid] = name

    return paths


# =========================
# 인스턴스 조회
# =========================
def list_instances(compartment_id):
    try:
        res = oci.pagination.list_call_get_all_results(
            compute_client.list_instances,
            compartment_id=compartment_id,
        )
        return res.data
    except Exception as e:
        print(f"⚠️ Instance 조회 실패: {compartment_id} / {e}")
        return []


# =========================
# VNIC / IP
# =========================
def get_vnic_and_ips(instance):
    if not instance or not instance.id:
        return None, None, None

    try:
        vnic_attachments = oci.pagination.list_call_get_all_results(
            compute_client.list_vnic_attachments,
            compartment_id=instance.compartment_id,
            instance_id=instance.id,
        ).data

        if len(vnic_attachments) == 0:
            return None, None, None

        primary_vnic_id = vnic_attachments[0].vnic_id
        vnic = network_client.get_vnic(primary_vnic_id).data

        private_ips = []
        public_ips = []

        if vnic.private_ip:
            private_ips.append(vnic.private_ip)
        if vnic.public_ip:
            public_ips.append(vnic.public_ip)

        return primary_vnic_id, ",".join(private_ips), ",".join(public_ips)

    except Exception:
        return None, None, None


# =========================
# UPSERT
# =========================
def upsert_instance(cur, row):
    sql = """
    INSERT INTO oci_instance_inventory (
      instance_id, tenancy_ocid, region,
      compartment_id, compartment_name, compartment_path,
      display_name, shape, ocpus, memory_gbs,
      lifecycle_state, availability_domain,
      primary_vnic_id, private_ips, public_ips,
      time_created_utc, last_refreshed_utc
    ) VALUES (
      %(instance_id)s, %(tenancy_ocid)s, %(region)s,
      %(compartment_id)s, %(compartment_name)s, %(compartment_path)s,
      %(display_name)s, %(shape)s, %(ocpus)s, %(memory_gbs)s,
      %(lifecycle_state)s, %(availability_domain)s,
      %(primary_vnic_id)s, %(private_ips)s, %(public_ips)s,
      %(time_created_utc)s, %(last_refreshed_utc)s
    )
    ON DUPLICATE KEY UPDATE
      compartment_name   = VALUES(compartment_name),
      compartment_path   = VALUES(compartment_path),
      display_name       = VALUES(display_name),
      shape              = VALUES(shape),
      ocpus              = VALUES(ocpus),
      memory_gbs         = VALUES(memory_gbs),
      lifecycle_state    = VALUES(lifecycle_state),
      availability_domain= VALUES(availability_domain),
      primary_vnic_id    = VALUES(primary_vnic_id),
      private_ips        = VALUES(private_ips),
      public_ips         = VALUES(public_ips),
      time_created_utc   = VALUES(time_created_utc),
      last_refreshed_utc = VALUES(last_refreshed_utc);
    """
    cur.execute(sql, row)


# =========================
# Main
# =========================
def main():
    try:
        init_schema(cur)
        logger.info("init_schema() 완료")
    except Exception as e:
        logger.error(f"init_schema() 처리 중 오류 발생: {e}")
        raise

    # 이번 실행 기준 타임스탬프 (초 단위로 맞춤)
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

    try:
        comp_map = load_compartments_with_root()
        logger.info("load_compartments_with_root() 완료")
    except Exception as e:
        logger.error(f"load_compartments_with_root() 처리 중 오류 발생: {e}")
        raise

    try:
        comp_paths = build_compartment_paths(comp_map)
        logger.info("build_compartment_paths() 완료")
    except Exception as e:
        logger.error(f"build_compartment_paths() 처리 중 오류 발생: {e}")
        raise

    active_comps = {
        cid: c for cid, c in comp_map.items()
        if cid != tenancy_ocid and getattr(c, "lifecycle_state", "ACTIVE") == "ACTIVE"
    }

    print(f"✔ ACTIVE 컴파트먼트 {len(active_comps)}개 조회")

    logger.info("테넌시 내에 있는 인스턴스 목록 수집 시작")

    # 👉 이번 실행에서 실제로 조회/업데이트된 인스턴스 ID 모음
    seen_instance_ids = set()

    for comp_id, comp in active_comps.items():
        comp_path = comp_paths.get(comp_id)
        comp_name = comp.name

        try:
            instances = list_instances(comp_id)
        except Exception as e:
            logger.error(f"list_instances() 중 오류 발생: {e}")
            raise

        if not instances:
            continue

        print(f"✔ {comp_path or comp_name} 에서 인스턴스 {len(instances)}개 조회")

        for inst in instances:
            primary_vnic_id, private_ips, public_ips = get_vnic_and_ips(inst)

            row = {
                "instance_id": inst.id,
                "tenancy_ocid": tenancy_ocid,
                "region": region,
                "compartment_id": comp_id,
                "compartment_name": comp_name,
                "compartment_path": comp_path,
                "display_name": inst.display_name,
                "shape": inst.shape,
                "ocpus": getattr(inst.shape_config, "ocpus", None),
                "memory_gbs": getattr(inst.shape_config, "memory_in_gbs", None),
                "lifecycle_state": inst.lifecycle_state,
                "availability_domain": inst.availability_domain,
                "primary_vnic_id": primary_vnic_id,
                "private_ips": private_ips,
                "public_ips": public_ips,
                "time_created_utc": inst.time_created.replace(tzinfo=None)
                    if inst.time_created else None,
                "last_refreshed_utc": run_ts,
            }
            try:
                upsert_instance(cur, row)
                seen_instance_ids.add(inst.id)
            except Exception as e:
                logger.error(f"upsert_instance() 중 오류 발생: {e}")
                raise

    # 🔥 이번 실행에서 한 번도 보이지 않은 인스턴스는 삭제
    try:
        if seen_instance_ids:
            placeholders = ",".join(["%s"] * len(seen_instance_ids))
            delete_sql = f"""
            DELETE FROM oci_instance_inventory
            WHERE tenancy_ocid = %s
              AND region = %s
              AND instance_id NOT IN ({placeholders})
            """
            params = [tenancy_ocid, region, *seen_instance_ids]
            cur.execute(delete_sql, params)
            logger.info(f"삭제된 인스턴스 정리 완료, {cur.rowcount}개 행 삭제")
        else:
            # 이번 실행에서 인스턴스가 하나도 안 조회되면, 해당 리전/테넌시 전체 삭제
            delete_sql = """
            DELETE FROM oci_instance_inventory
            WHERE tenancy_ocid = %s
              AND region = %s
            """
            cur.execute(delete_sql, (tenancy_ocid, region))
            logger.info(f"이번 실행에서 인스턴스가 조회되지 않아, {cur.rowcount}개 행 전체 삭제")
    except Exception as e:
        logger.error(f"삭제된 인스턴스 정리 중 오류 발생: {e}")
        raise

    logger.info("테넌시 내에 있는 인스턴스 목록 저장 완료")


if __name__ == "__main__":
    main()

