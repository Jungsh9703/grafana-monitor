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
        logging.FileHandler("/grafana_python/instance_principal_v/logs/lb_list.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("lb_list")

# DB 접속 정보 불러오기
load_dotenv()

# MySQL 접속 정보
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")


# =========================
# MySQL 연결 설정
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
    logger.error(f"MySQL 연결  중 오류 발생: {e}")
    raise

# =========================
# OCI 설정 (Instance Principal)
# =========================
try:
    signer = InstancePrincipalsSecurityTokenSigner()
    region = os.getenv("OCI_REGION") or "ap-seoul-1"
    config = {"region": region}

    tenancy_ocid = os.getenv("TENANCY_OCID")

    lb_client = oci.load_balancer.LoadBalancerClient(config, signer=signer)
    identity_client = oci.identity.IdentityClient(config, signer=signer)

    # 🔹 테넌시 이름
    tenancy = identity_client.get_tenancy(tenancy_ocid).data
    TENANCY_NAME = tenancy.name   # 필요시 "gtopn" 고정 가능
    logger.info("API 연결 완료")
except Exception as e:
    logger.error(f"API 연결 중 오류 발생: {e}")
    raise

# =========================
# 테이블 없으면 생성
# =========================
def init_schema(cur):
    ddl = """
    CREATE TABLE IF NOT EXISTS oci_lb_inventory (
      lb_id              VARCHAR(200)  NOT NULL,
      tenancy_ocid       VARCHAR(128)  NOT NULL,
      region             VARCHAR(64)   NOT NULL,
      compartment_id     VARCHAR(200)  NOT NULL,
      compartment_name   VARCHAR(255)  NULL,
      compartment_path   VARCHAR(500)  NULL,

      display_name       VARCHAR(255)  NOT NULL,
      shape_name         VARCHAR(64)   NOT NULL,
      is_private         TINYINT(1)    NOT NULL,   -- 0: public, 1: private
      ip_mode            VARCHAR(32)   NULL,       -- IPV4 / IPV4_AND_IPV6
      lifecycle_state    VARCHAR(32)   NOT NULL,

      subnet_ids         TEXT          NULL,       -- 콤마/JSON 등으로 저장
      reserved_ips       TEXT          NULL,

      time_created_utc   DATETIME      NULL,
      last_refreshed_utc DATETIME      NOT NULL,

      PRIMARY KEY (lb_id),
      KEY idx_lb_compartment (compartment_id),
      KEY idx_lb_region      (region),
      KEY idx_lb_state       (lifecycle_state),
      KEY idx_lb_display     (display_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    cur.execute(ddl)

# =========================
# 컴파트먼트 전체 로드
# =========================
def load_compartments_with_parents():
    """
    테넌시 전체 컴파트먼트 로드
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
    - 루트(tenancy): TENANCY_NAME
    - 루트 직속:     TENANCY_NAME > cloudteam
    - 그 아래:       cloudteam > cwchoi
    """
    paths = {}

    for cid, c in comp_map.items():
        if cid == tenancy_ocid:
            # 루트는 LB가 직접 속하지 않으므로 path에서 제외
            continue

        name = c.name
        parent_id = getattr(c, "compartment_id", None)

        if parent_id is None or parent_id == tenancy_ocid:
            paths[cid] = f"{TENANCY_NAME}{separator}{name}"
        else:
            parent = comp_map.get(parent_id)
            if parent:
                paths[cid] = f"{parent.name}{separator}{name}"
            else:
                paths[cid] = name

    return paths

# =========================
# Load Balancer 조회
# =========================
def list_lbs(compartment_id):
    try:
        res = oci.pagination.list_call_get_all_results(
            lb_client.list_load_balancers,
            compartment_id=compartment_id,
        )
        return res.data
    except Exception as e:
        print(f"⚠️ LB 조회 실패: {compartment_id} / {e}")
        return []

# =========================
# UPSERT 저장
# =========================
def upsert_lb(cur, row):
    sql = """
    INSERT INTO oci_lb_inventory (
      lb_id,
      tenancy_ocid,
      region,
      compartment_id,
      compartment_name,
      compartment_path,
      display_name,
      shape_name,
      is_private,
      ip_mode,
      lifecycle_state,
      subnet_ids,
      reserved_ips,
      time_created_utc,
      last_refreshed_utc
    ) VALUES (
      %(lb_id)s,
      %(tenancy_ocid)s,
      %(region)s,
      %(compartment_id)s,
      %(compartment_name)s,
      %(compartment_path)s,
      %(display_name)s,
      %(shape_name)s,
      %(is_private)s,
      %(ip_mode)s,
      %(lifecycle_state)s,
      %(subnet_ids)s,
      %(reserved_ips)s,
      %(time_created_utc)s,
      %(last_refreshed_utc)s
    )
    ON DUPLICATE KEY UPDATE
      compartment_name   = VALUES(compartment_name),
      compartment_path   = VALUES(compartment_path),
      display_name       = VALUES(display_name),
      shape_name         = VALUES(shape_name),
      is_private         = VALUES(is_private),
      ip_mode            = VALUES(ip_mode),
      lifecycle_state    = VALUES(lifecycle_state),
      subnet_ids         = VALUES(subnet_ids),
      reserved_ips       = VALUES(reserved_ips),
      time_created_utc   = VALUES(time_created_utc),
      last_refreshed_utc = VALUES(last_refreshed_utc);
    """
    cur.execute(sql, row)

# =========================
# Main
# =========================
def main():
    # 테이블 없으면 생성
    try:
        init_schema(cur)
        logger.info("테이블 초기 셋팅 완료")
    except Exception as e:
        logger.error(f"init_schema() 처리 중 오류 발생: {e}")
        raise

    # 이번 실행 기준 타임스탬프(마이크로초 제거)
    run_ts = datetime.utcnow().replace(microsecond=0, tzinfo=None)

    # 1) 컴파트먼트 전체 + 2단계 path 계산
    try:
        comp_map = load_compartments_with_parents()
        logger.info("load_compartments_with_parents() 완료")
    except Exception as e:
        logger.error(f"load_compartments_with_parents() 처리 중 오류 발생: {e}")
        raise

    try:
        comp_paths = build_compartment_paths(comp_map, separator=" > ")
        logger.info("build_compartment_paths() 완료")
    except Exception as e:
        logger.error(f"build_compartment_paths() 처리 중 오류 발생: {e}")
        raise

    # 2) ACTIVE 컴파트먼트만 대상으로 루프 (루트 제외)
    compartments = [
        c for cid, c in comp_map.items()
        if cid != tenancy_ocid and getattr(c, "lifecycle_state", None) == "ACTIVE"
    ]

    print(f"✔ ACTIVE 컴파트먼트 수: {len(compartments)}")
    logger.info("Load Balancer 목록 수집 시작")

    # 👉 이번 실행에서 실제로 조회된 LB ID 모음
    seen_lb_ids = set()

    for comp in compartments:
        comp_id = comp.id
        comp_name = comp.name
        comp_path = comp_paths.get(comp_id)
        try:
            lbs = list_lbs(comp_id)
        except Exception as e:
            logger.error(f"list_lbs() 처리 중 오류 발생: {e}")
            raise

        if not lbs:
            continue

        print(f"✔ {comp_path} ({comp_id}) 에서 LB {len(lbs)}개 조회")

        for lb in lbs:
            # subnet_ids 문자열로 저장
            subnet_ids = ",".join(lb.subnet_ids) if lb.subnet_ids else None

            # reserved_ips 추출
            reserved_ips = None
            if lb.ip_addresses:
                rp = [
                    ip.reserved_ip.id
                    for ip in lb.ip_addresses
                    if getattr(ip, "reserved_ip", None)
                ]
                if rp:
                    reserved_ips = ",".join(rp)

            row = {
                "lb_id": lb.id,
                "tenancy_ocid": tenancy_ocid,
                "region": region,
                "compartment_id": comp_id,
                "compartment_name": comp_name,
                "compartment_path": comp_path,  # ✅ 2단계 path
                "display_name": lb.display_name,
                "shape_name": lb.shape_name,
                "is_private": 1 if lb.is_private else 0,
                "ip_mode": getattr(lb, "ip_mode", None),
                "lifecycle_state": lb.lifecycle_state,
                "subnet_ids": subnet_ids,
                "reserved_ips": reserved_ips,
                "time_created_utc": lb.time_created.replace(tzinfo=None) if lb.time_created else None,
                "last_refreshed_utc": run_ts,
            }
            try:
                upsert_lb(cur, row)
                seen_lb_ids.add(lb.id)
            except Exception as e:
                logger.error(f"upsert_lb() 처리 중 오류 발생: {e}")
                raise

    # 🔥 이번 실행에서 한 번도 보이지 않은 LB는 삭제
    try:
        if seen_lb_ids:
            placeholders = ",".join(["%s"] * len(seen_lb_ids))
            delete_sql = f"""
            DELETE FROM oci_lb_inventory
            WHERE tenancy_ocid = %s
              AND region = %s
              AND lb_id NOT IN ({placeholders})
            """
            params = [tenancy_ocid, region, *seen_lb_ids]
            cur.execute(delete_sql, params)
            logger.info(f"삭제된 LB 정리 완료, {cur.rowcount}개 행 삭제")
        else:
            # 이번 실행에서 LB가 하나도 조회되지 않으면, 해당 테넌시/리전 전체 삭제
            delete_sql = """
            DELETE FROM oci_lb_inventory
            WHERE tenancy_ocid = %s
              AND region = %s
            """
            cur.execute(delete_sql, (tenancy_ocid, region))
            logger.info(f"이번 실행에서 LB가 조회되지 않아, {cur.rowcount}개 행 전체 삭제")
    except Exception as e:
        logger.error(f"삭제된 LB 정리 중 오류 발생: {e}")
        raise

    logger.info("Load Balancer 목록 저장 완료")


if __name__ == "__main__":
    main()

