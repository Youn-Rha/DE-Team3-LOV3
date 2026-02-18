#!/bin/bash
# ============================================================
# Spark 설정 파일 배포 스크립트
# Master 노드에서 실행 — 자기 자신 + Worker에 설정 배포
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIGS_DIR="${SCRIPT_DIR}/../spark_configs"
source "${SCRIPT_DIR}/../../env.sh" 2>/dev/null || {
    echo "ERROR: env.sh 파일을 찾을 수 없습니다."
    exit 1
}

echo "========================================="
echo "Spark 설정 파일 배포"
echo "========================================="

SPARK_CONF="/opt/spark/conf"

# sed로 IP 변수 치환하는 함수
apply_template() {
    local src="$1"
    local dest="$2"
    sed \
        -e "s|\${MASTER_PRIVATE_IP}|${MASTER_PRIVATE_IP}|g" \
        -e "s|\${WORKER1_PRIVATE_IP}|${WORKER1_PRIVATE_IP}|g" \
        -e "s|\${WORKER2_PRIVATE_IP}|${WORKER2_PRIVATE_IP}|g" \
        "$src" > "$dest"
}

# 임시 디렉토리
TMP_DIR=$(mktemp -d)
trap "rm -rf ${TMP_DIR}" EXIT

# 1. Master 노드 (로컬에 직접 복사)
echo "[1/2] Master (${MASTER_PRIVATE_IP}) 설정 배포 중..."
apply_template "${CONFIGS_DIR}/spark-env.sh.master" "${SPARK_CONF}/spark-env.sh"
apply_template "${CONFIGS_DIR}/spark-defaults.conf" "${SPARK_CONF}/spark-defaults.conf"
apply_template "${CONFIGS_DIR}/workers" "${SPARK_CONF}/workers"
echo "  Master 배포 완료"

# 2. Worker 노드 (SSH로 전송)
echo "[2/2] Worker 노드 설정 배포 중..."
for WORKER_IP in "${WORKER1_PRIVATE_IP}" "${WORKER2_PRIVATE_IP}"; do
    echo "  → ${WORKER_IP} 배포 중..."
    apply_template "${CONFIGS_DIR}/spark-env.sh.worker" "${TMP_DIR}/spark-env.sh"
    apply_template "${CONFIGS_DIR}/spark-defaults.conf" "${TMP_DIR}/spark-defaults.conf"

    scp "${TMP_DIR}/spark-env.sh" "${SSH_USER}@${WORKER_IP}:${SPARK_CONF}/spark-env.sh"
    scp "${TMP_DIR}/spark-defaults.conf" "${SSH_USER}@${WORKER_IP}:${SPARK_CONF}/spark-defaults.conf"
    echo "    완료"
done

echo "========================================="
echo "설정 배포 완료!"
echo "========================================="
echo ""
echo "검증:"
echo "  cat ${SPARK_CONF}/spark-env.sh"
echo "  cat ${SPARK_CONF}/workers"
echo "  ssh ${SSH_USER}@${WORKER1_PRIVATE_IP} 'cat ${SPARK_CONF}/spark-env.sh'"
