import sys
import mysql.connector
import oci
from datetime import datetime, timedelta
import os
from oci.auth.signers import InstancePrincipalsSecurityTokenSigner
import logging
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] - [%(levelname)s] %(message)s in %(filename)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("/grafana_python/instance_principal_v/logs/insert_uc_contract.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("insert_uc_contract")

# =======================
# DB 접속 정보
# =======================
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# =======================
# 0️⃣ 실행 기준일
# =======================
base_date = datetime.utcnow().date()

# =======================
# 1️⃣ OCI 설정 (Instance Principal)
# =======================
try:
    signer = InstancePrincipalsSecurityTokenSigner()

    region = os.getenv("OCI_REGION") or "ap-seoul-1"
    config = {"region": region}

    tenancy_ocid = os.getenv("TENANCY_OCID")
    if not tenancy_ocid:
        raise RuntimeError("환경변수 TENANCY_OCID 미설정")

    logger.info("OCI Instance Principal 인증 완료")
except Exception as e:
    logger.error(f"OCI 인증 오류: {e}")
    raise

# =======================
# 2️⃣ UC 계약 조회
# =======================
try:
    from oci.onesubscription import (
        OrganizationSubscriptionClient,
        SubscribedServiceClient,
    )

    org_client = OrganizationSubscriptionClient(config, signer=signer)
    svc_client = SubscribedServiceClient(config, signer=signer)

    org_subs = org_client.list_organization_subscriptions(
        compartment_id=tenancy_ocid
    ).data

    if not org_subs:
        raise RuntimeError("Organization Subscription 없음")

    subscription_id = org_subs[0].id

    services = svc_client.list_subscribed_services(
        compartment_id=tenancy_ocid,
        subscription_id=subscription_id
    ).data

    total_credits = 0.0
    used_credits = 0.0
    left_credits = 0.0
    start_dates = []
    end_dates = []
    invoice_dates = []

    for s in services:
        if s.status != "ACTIVE":
            continue

        total_credits += float(s.quantity or 0)
        used_credits += float(s.used_amount or 0)
        left_credits += float(s.available_amount or 0)

        start_dates.append(s.time_majorset_start)
        end_dates.append(s.time_majorset_end)
        invoice_dates.append(s.time_created)

    if not start_dates:
        raise RuntimeError("ACTIVE UC 계약 없음")

    logger.info("UC 계약 정보 수집 완료")

except Exception as e:
    logger.error(f"UC 계약 조회 오류: {e}")
    raise

# =======================
# 3️⃣ MySQL 연결
# =======================
try:
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
    )
    cursor = conn.cursor()
    logger.info("MySQL 연결 완료")
except Exception as e:
    logger.error(f"MySQL 연결 오류: {e}")
    raise

# =======================
# 4️⃣ 테이블 생성
# =======================
try:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS contracts (
            base_date DATE PRIMARY KEY,
            invoice_date DATETIME,
            start_date DATETIME,
            end_date DATETIME,
            total_credits INT,
            used_credits INT,
            left_credits INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    logger.info("UC 계약 테이블 확인/생성 완료")
except Exception as e:
    logger.error(f"테이블 생성 오류: {e}")
    raise

# =======================
# 5️⃣ 기존 데이터 삭제
# =======================
try:
    cursor.execute(
        "DELETE FROM contracts WHERE base_date = %s",
        (base_date,)
    )
    logger.info(f"{base_date} 기존 UC 데이터 삭제 완료")
except Exception as e:
    logger.error(f"기존 데이터 삭제 오류: {e}")
    raise

# =======================
# 6️⃣ 신규 데이터 삽입
# =======================
try:
    cursor.execute(
        """
        INSERT INTO contracts
        (base_date, invoice_date, start_date, end_date,
         total_credits, used_credits, left_credits)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            base_date,
            min(invoice_dates),
            min(start_dates),
            max(end_dates),
            round(total_credits),
            round(used_credits),
            round(left_credits),
        )
    )

    conn.commit()
    logger.info("UC 계약 정보 DB 저장 완료")

    cursor.close()
    conn.close()

except Exception as e:
    logger.error(f"UC 계약 데이터 저장 오류: {e}")
    raise

# =======================
# 🔚 종료
# =======================
print("[INFO] UC 계약 정보 수집 및 저장 완료")
