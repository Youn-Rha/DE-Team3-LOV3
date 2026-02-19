#!/bin/bash
# ============================================================
# Spark 클러스터 시작 스크립트
# EC2 인스턴스 시작 → Spark 클러스터 시작
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../../env.sh" 2>/dev/null || {
    echo "ERROR: env.sh 파일을 찾을 수 없습니다."
    exit 1
}

echo "========================================="
echo "Spark 클러스터 시작"
echo "========================================="

# 1. EC2 인스턴스 시작
echo "[1/3] EC2 인스턴스 시작 중..."
aws ec2 start-instances \
    --instance-ids ${MASTER_INSTANCE_ID} ${WORKER1_INSTANCE_ID} ${WORKER2_INSTANCE_ID} ${WORKER3_INSTANCE_ID} ${WORKER4_INSTANCE_ID} \
    --region ${AWS_REGION}

# 2. 인스턴스가 running 상태가 될 때까지 대기
echo "[2/3] 인스턴스 시작 대기 중..."
aws ec2 wait instance-running \
    --instance-ids ${MASTER_INSTANCE_ID} ${WORKER1_INSTANCE_ID} ${WORKER2_INSTANCE_ID} ${WORKER3_INSTANCE_ID} ${WORKER4_INSTANCE_ID} \
    --region ${AWS_REGION}

echo "모든 인스턴스가 시작되었습니다. 30초 대기 중..."
sleep 30

# 3. Spark 클러스터 시작 (Master에서 start-all.sh)
echo "[3/3] Spark 클러스터 시작 중..."
ssh -i "${SSH_KEY_PATH}" "${SSH_USER}@${MASTER_PRIVATE_IP}" \
    "source ~/.bashrc && ${SPARK_HOME}/sbin/start-all.sh"

echo "========================================="
echo "클러스터 시작 완료!"
echo "========================================="
echo ""
echo "Spark Master UI: http://${MASTER_PRIVATE_IP}:8080"
echo ""
echo "상태 확인:"
echo "  ssh -i ${SSH_KEY_PATH} ${SSH_USER}@${MASTER_PRIVATE_IP} 'jps'"
