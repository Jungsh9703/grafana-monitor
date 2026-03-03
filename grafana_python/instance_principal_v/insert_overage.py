import sys
import mysql.connector
import oci
from datetime import datetime, timedelta
import os
from oci.auth.signers import InstancePrincipalsSecurityTokenSigner
import logging
from dotenv import load_dotenv
import argparse

# =======================
# 로깅 설정
# =======================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] - [%(levelname)s] %(message)s in %(filename)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("/grafana_python/instance_principal_v/logs/insert_overage.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("insert_overage")

# =======================
# 0️⃣ 실행 기준일 및 기간 설정
# =======================
parser = argparse.ArgumentParser(description='OCI Overage Data Batch Collection')
parser.add_argument('--start_date', type=str, help="시작 날짜 (YYYY-MM-DD)")
parser.add_argument('--end_date', type=str, help="종료 날짜 (YYYY-MM-DD)")
args = parser.parse_args()

date_list = []

if args.start_date and args.end_date:
    try:
        start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.end_date, "%Y-%m-%d").date()

        curr = start
        while curr <= end:
            date_list.append(curr.strftime('%Y-%m-%d'))
            curr += timedelta(days=1)
        logger.info(f" 기간 모드 실행: {args.start_date} ~ {args.end_date} (총 {len(date_list)}일)")
    except ValueError:
        logger.error(" 날짜 형식이 잘못되었습니다. YYYY-MM-DD 형식을 사용하세요.")
        sys.exit(1)
else:
    target_date_str = (datetime.utcnow().date() - timedelta(days=1)).strftime('%Y-%m-%d')
    date_list.append(target_date_str)
    logger.info(f" 단일 날짜 모드 실행: {target_date_str}")

# =======================
# 1️⃣ 환경 변수 및 OCI/DB 준비
# =======================
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
TENANCY_OCID = os.getenv("TENANCY_OCID")
REGION = os.getenv("OCI_REGION") or "ap-seoul-1"
ENV_SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID")

try:
    signer = InstancePrincipalsSecurityTokenSigner()
    config = {"region": REGION}
    from oci.osub_usage import ComputedUsageClient
    usage_client = ComputedUsageClient(config, signer=signer)

    # DB 연결
    conn = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME
    )

    # --- 테이블 자동 생성 로직 추가 ---
    cursor = conn.cursor()
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS overage (
        id INT AUTO_INCREMENT PRIMARY KEY,
        base_date DATE NOT NULL,
        total_overage_amount DECIMAL(18, 4) DEFAULT 0.0000,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uk_base_date (base_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    # ------------------------------

    logger.info(f" OCI 인증 및 DB 연결 성공 (테이블 확인 완료)")
except Exception as e:
    logger.error(f" 초기화 오류: {e}")
    sys.exit(1)

# =======================
# 2️⃣ 날짜별 루프 실행
# =======================
try:
    for target_date_str in date_list:
        time_from_str = f"{target_date_str}T00:00:00Z"
        time_to_str = f"{target_date_str}T23:59:59Z"

        total_overage = 0.0
        item_count = 0

        # API 호출
        usage_response = usage_client.list_computed_usage_aggregateds(
            compartment_id=TENANCY_OCID,
            subscription_id=ENV_SUBSCRIPTION_ID,
            time_from=time_from_str,
            time_to=time_to_str
        )

        if usage_response.data:
            for group in usage_response.data:
                inner_usages = getattr(group, 'aggregated_computed_usages', [])
                for item in inner_usages:
                    # OVERAGE 타입 필터링
                    if getattr(item, 'type', '') == 'OVERAGE':
                        cost_val = float(getattr(item, 'cost_unrounded', 0) or getattr(item, 'cost', 0) or 0)
                        if cost_val > 0:
                            total_overage += cost_val
                            item_count += 1

        # 3️⃣ 데이터베이스 저장
        cursor = conn.cursor()
        # 중복 방지를 위한 Delete-Insert (Idempotency)
        cursor.execute("DELETE FROM overage WHERE base_date = %s", (target_date_str,))
        cursor.execute(
            "INSERT INTO overage (base_date, total_overage_amount) VALUES (%s, %s)",
            (target_date_str, total_overage)
        )
        conn.commit()
        cursor.close()

        logger.info(f" [{target_date_str}] 수집 완료: {item_count}건 / 합계 {total_overage:,.2f}원")

except Exception as e:
    logger.error(f" 루프 실행 중 오류 발생: {e}")
finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()
        logger.info(" DB 연결 종료")

logger.info(" 모든 작업이 완료되었습니다.")
