#!/bin/bash
# ============================================================
# 포트홀 파이프라인 실행 스크립트
#
# 사용법:
#   bash run_pipeline.sh              # 어제 날짜로 실행
#   bash run_pipeline.sh 2026-02-12   # 특정 날짜로 실행
#
# 하는 일:
#   1. 환경변수 로드 (infra/env.sh)
#   2. Airflow webserver 미실행 시 기동
#   3. Airflow scheduler 미실행 시 기동 (env 포함)
#   4. DAG 언파우즈
#   5. DAG 트리거 (trigger_dag.py)
# ============================================================

set -e

# ---- 경로 설정 ----
REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
AIRFLOW_HOME="/home/ec2-user/airflow"
VENV="/home/ec2-user/airflow-venv"
DAG_ID="pothole_pipeline_spark_standalone"

# ---- 환경변수 로드 ----
source "${REPO_DIR}/infra/env.sh"
export AIRFLOW_HOME

# ---- 날짜 결정 ----
if [ -n "$1" ]; then
    # 형식 검증
    if ! echo "$1" | grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'; then
        echo "ERROR: 날짜 형식 오류. 올바른 형식: YYYY-MM-DD (예: 2026-02-12)"
        exit 1
    fi
    BATCH_DATE="$1"
else
    BATCH_DATE=$(date -u -d "yesterday" +%Y-%m-%d 2>/dev/null || date -u -v-1d +%Y-%m-%d)
fi

echo "========================================"
echo " 포트홀 파이프라인 실행"
echo " 배치 날짜: ${BATCH_DATE}"
echo "========================================"

# ---- Airflow webserver 기동 ----
if pgrep -f "airflow webserver" > /dev/null 2>&1; then
    echo "[1/4] Airflow webserver: 이미 실행 중"
else
    echo "[1/4] Airflow webserver: 기동 중..."
    setsid bash -c "
        source ${HOME}/.bashrc
        source ${VENV}/bin/activate
        export AIRFLOW_HOME=${AIRFLOW_HOME}
        airflow webserver -p 8080 >> ${AIRFLOW_HOME}/logs/webserver.log 2>&1
    " &
    sleep 5
    echo "       webserver 기동 완료 (http://localhost:8080)"
fi

# ---- Airflow scheduler 기동 ----
if pgrep -f "airflow scheduler" > /dev/null 2>&1; then
    echo "[2/4] Airflow scheduler: 이미 실행 중"
else
    echo "[2/4] Airflow scheduler: 기동 중..."
    setsid bash -c "
        source ${HOME}/.bashrc
        source ${VENV}/bin/activate
        export AIRFLOW_HOME=${AIRFLOW_HOME}
        airflow scheduler >> ${AIRFLOW_HOME}/logs/scheduler.log 2>&1
    " &
    echo "       scheduler 기동 대기 중 (15초)..."
    sleep 15
    if ! pgrep -f "airflow scheduler" > /dev/null 2>&1; then
        echo "ERROR: scheduler 기동 실패. 로그 확인: ${AIRFLOW_HOME}/logs/scheduler.log"
        exit 1
    fi
    echo "       scheduler 기동 완료"
fi

# ---- DAG 언파우즈 ----
echo "[3/4] DAG 언파우즈..."
source "${VENV}/bin/activate"
airflow dags unpause "${DAG_ID}" 2>/dev/null | grep -v UserWarning || true

# ---- DAG 트리거 ----
echo "[4/4] DAG 트리거: ${BATCH_DATE}"
python3 "${REPO_DIR}/airflow_service/scripts/trigger_dag.py" "${BATCH_DATE}"

echo ""
echo "========================================"
echo " 트리거 완료!"
echo " Airflow UI: http://localhost:8080"
echo " DAG: ${DAG_ID}"
echo " 배치 날짜: ${BATCH_DATE}"
echo "========================================"
