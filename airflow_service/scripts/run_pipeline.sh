#!/bin/bash
# ============================================================
# 포트홀 파이프라인 실행 스크립트
#
# 사용법:
#   bash run_pipeline.sh                      # 어제 날짜로 실행
#   bash run_pipeline.sh 2026-02-12           # 특정 날짜로 실행
#   bash run_pipeline.sh 2026-02-12 --force   # 기존 run 삭제 후 재실행
#
# 하는 일:
#   1. 환경변수 로드 (infra/env.sh)
#   2. Airflow webserver 미실행 시 기동
#   3. Airflow scheduler 미실행 시 기동 (env 포함)
#   4. DAG 언파우즈
#   5. --force 시 기존 동일 날짜 run 삭제
#   6. DAG 트리거 (trigger_dag.py)
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

# ---- 인자 파싱 ----
BATCH_DATE=""
FORCE=0
for arg in "$@"; do
    if [ "$arg" = "--force" ]; then
        FORCE=1
    elif echo "$arg" | grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'; then
        BATCH_DATE="$arg"
    elif [ -n "$arg" ]; then
        echo "ERROR: 알 수 없는 인자: $arg"
        echo "사용법: bash run_pipeline.sh [YYYY-MM-DD] [--force]"
        exit 1
    fi
done

if [ -z "$BATCH_DATE" ]; then
    BATCH_DATE=$(date -u -d "yesterday" +%Y-%m-%d 2>/dev/null || date -u -v-1d +%Y-%m-%d)
fi

echo "========================================"
echo " 포트홀 파이프라인 실행"
echo " 배치 날짜: ${BATCH_DATE}"
[ "$FORCE" -eq 1 ] && echo " 모드: --force (기존 run 삭제 후 재실행)"
echo "========================================"

# ---- Airflow webserver 기동 ----
if pgrep -f "airflow webserver" > /dev/null 2>&1; then
    echo "[1/5] Airflow webserver: 이미 실행 중"
else
    echo "[1/5] Airflow webserver: 기동 중..."
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
    echo "[2/5] Airflow scheduler: 이미 실행 중"
else
    echo "[2/5] Airflow scheduler: 기동 중..."
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
echo "[3/5] DAG 언파우즈..."
source "${VENV}/bin/activate"
airflow dags unpause "${DAG_ID}" 2>/dev/null | grep -v UserWarning || true

# ---- --force: 기존 run 삭제 ----
if [ "$FORCE" -eq 1 ]; then
    echo "[4/5] 기존 run 삭제 중 (${BATCH_DATE})..."
    python3 -c "
import sys
sys.path.insert(0, '.')
from airflow import settings
from airflow.models import DagRun, TaskInstance
s = settings.Session()
run_id = 'manual__${BATCH_DATE}T00:00:00+00:00'
dag_id = '${DAG_ID}'
deleted_ti = s.query(TaskInstance).filter_by(dag_id=dag_id, run_id=run_id).delete()
deleted_dr = s.query(DagRun).filter_by(dag_id=dag_id, run_id=run_id).delete()
s.commit()
s.close()
if deleted_dr:
    print(f'       삭제 완료: {deleted_dr} run, {deleted_ti} tasks')
else:
    print('       삭제할 기존 run 없음')
"
else
    echo "[4/5] 기존 run 보존 (--force 미사용)"
fi

# ---- DAG 트리거 ----
echo "[5/5] DAG 트리거: ${BATCH_DATE}"
python3 "${REPO_DIR}/airflow_service/scripts/trigger_dag.py" "${BATCH_DATE}"

echo ""
echo "========================================"
echo " 트리거 완료!"
echo " Airflow UI: http://localhost:8080"
echo " DAG: ${DAG_ID}"
echo " 배치 날짜: ${BATCH_DATE}"
echo "========================================"
