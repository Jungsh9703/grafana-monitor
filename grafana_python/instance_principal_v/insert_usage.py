import sys
import mysql.connector
import oci
from datetime import datetime, timedelta, timezone
import os
from oci.auth.signers import InstancePrincipalsSecurityTokenSigner
import logging
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] - [%(levelname)s] %(message)s in %(filename)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("/grafana_python/instance_principal_v/logs/insert_usage.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("insert_usage")

# DB 접속 정보 불러오기
load_dotenv()

# MySQL 접속 정보
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# =======================
# 0️⃣ 실행 파라미터
# =======================
# 없으면 전일 자동 계산
if len(sys.argv) >= 2:
    target_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
else:
    target_date = (datetime.utcnow() - timedelta(days=1)).date()

next_date = target_date + timedelta(days=1)

# =======================
# 1️⃣ OCI 설정 (Instance Principal)
# =======================
try:
    # Instance Principal signer
    signer = InstancePrincipalsSecurityTokenSigner()

    # region: OCI_REGION 환경변수 우선, 없으면 signer.region, 그래도 없으면 기본값
    region = os.getenv("OCI_REGION")
    if not region:
        # 사용량/비용 조회는 보통 홈 리전 기준이니까,
        # 여기에 홈 리전 값을 넣어두면 됨 (예: ap-seoul-1)
        region = "ap-seoul-1"

    config = {"region": region}

    # 테넌시 OCID (환경변수로 설정 필요)
    tenancy_ocid = os.getenv("TENANCY_OCID")
    if not tenancy_ocid:
        raise RuntimeError("환경변수 TENANCY_OCID 를 설정해야 합니다.")

    # Usage API 클라이언트
    usage_client = oci.usage_api.UsageapiClient(config, signer=signer)
    logger.info("API 연결 완료")
except Exception as e:
    logger.error(f"API 연결 중 오류 발생: {e}")
    raise

# =======================
# 2️⃣ Usage API 호출
# =======================
try:
    request = oci.usage_api.models.RequestSummarizedUsagesDetails(
        tenant_id=tenancy_ocid,
        granularity="DAILY",
        query_type="COST",
        time_usage_started=f"{target_date}T00:00:00Z",
        time_usage_ended=f"{next_date}T00:00:00Z",
        group_by=["service"],
    )

    response = usage_client.request_summarized_usages(request)
    logger.info("리소스별  UC Daily 사용량 수집 완료")
except Exception as e:
    logger.error(f"리소스별  UC Daily 사용량 수집 중 오류 발생: {e}")
    raise

# =======================
# 3️⃣ 전처리 및 중복 서비스 합산
# =======================
raw_items = []
for item in response.data.items:
    raw_items.append(
        {
            "usage_date": target_date.strftime("%Y-%m-%d"),
            "service": (item.service or "Unknown").strip(),
            "computed_amount": float(item.computed_amount or 0),
            "currency": "KRW",
        }
    )

# 동일한 서비스명 합산
aggregated = {}
for r in raw_items:
    key = (r["usage_date"], r["service"])
    aggregated[key] = aggregated.get(key, 0) + r["computed_amount"]

parsed = [
    {"usage_date": k[0], "service": k[1], "computed_amount": v, "currency": "KRW"}
    for k, v in aggregated.items()
]

# =======================
# 4️⃣ 콘솔 출력
# =======================
print(f"\n[ {target_date} 서비스별 사용금액 (중복 합산 후) ]\n")
print(f"{'Service':40s} {'Amount':>15s} {'Currency':>10s}")
print("-" * 70)
for r in parsed:
    print(f"{r['service']:<40s} {r['computed_amount']:>15,.2f} {r['currency']:>10s}")
print("-" * 70)
total_cost = sum(r["computed_amount"] for r in parsed)
print(
    f"{'총합':<40s} {total_cost:>15,.2f} "
    f"{parsed[0]['currency'] if parsed else 'KRW':>10s}"
)

# =======================
# 5️⃣ MySQL 연결
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
    logger.error(f"MySQL 연결 중 오류 발생: {e}")
    raise

# =======================
# 6️⃣ 테이블 생성 (없을 경우 자동 생성)
# =======================
try:
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS oci_api_cost_daily_service_report (
        id INT AUTO_INCREMENT PRIMARY KEY,
        usage_date DATE NOT NULL,
        service VARCHAR(100) NOT NULL,
        computed_amount DECIMAL(18,6) DEFAULT 0,
        currency VARCHAR(10) DEFAULT 'KRW',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_usage_service (usage_date, service)
    )
    """
    )
    logger.info("테이블 초기 셋팅 완료")
    print("[INFO] Checked or created table: oci_api_cost_daily_service_report")
except Exception as e:
    logger.error("테이블 초기 셋팅 중 오류 발생: {e}")
    raise

# =======================
# 7️⃣ 기존 데이터 삭제 후 신규 삽입
# =======================
try:
    cursor.execute(
        "DELETE FROM oci_api_cost_daily_service_report WHERE usage_date = %s",
        (target_date,),
    )
    print(f"[INFO] Deleted existing records for {target_date}")

    insert_sql = """
    INSERT INTO oci_api_cost_daily_service_report
        (usage_date, service, computed_amount, currency)
    VALUES (%s, %s, %s, %s)
    """

    for r in parsed:
        cursor.execute(
            insert_sql,
            (r["usage_date"], r["service"], r["computed_amount"], r["currency"]),
        )

    conn.commit()
    print(f"[INFO] Inserted {len(parsed)} aggregated service records for {target_date}")

    # =======================
    # 🔚 종료
    # =======================
    cursor.close()
    conn.close()
    print("\n[INFO] Daily usage insert completed successfully.")
    logger.info("리소스별  UC Daily 사용량 저장 완료")
except Exception as e:
    logger.error("리소스별  UC Daily 사용량 저장 중 오류 발생: {e}")
    raise