#!/bin/bash
# ============================================================
# Spark 클러스터 종료 스크립트
# Spark 클러스터 종료 → EC2 인스턴스 중지
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../../env.sh" 2>/dev/null || {
    echo "ERROR: env.sh 파일을 찾을 수 없습니다."
    exit 1
}

echo "========================================="
echo "Spark 클러스터 종료"
echo "========================================="

# 1. Spark 클러스터 종료
echo "[1/2] Spark 클러스터 종료 중..."
ssh -i "${SSH_KEY_PATH}" "${SSH_USER}@${MASTER_PRIVATE_IP}" \
    "source ~/.bashrc && ${SPARK_HOME}/sbin/stop-all.sh" || true

sleep 5

# 2. EC2 인스턴스 중지
echo "[2/2] EC2 인스턴스 중지 중..."
aws ec2 stop-instances \
    --instance-ids ${MASTER_INSTANCE_ID} ${WORKER1_INSTANCE_ID} ${WORKER2_INSTANCE_ID} \
    --region ${AWS_REGION}

echo "========================================="
echo "클러스터 종료 완료!"
echo "========================================="
echo ""
echo "비용 절감: EC2 인스턴스가 중지되었습니다."
echo "재시작: bash start_cluster.sh"
