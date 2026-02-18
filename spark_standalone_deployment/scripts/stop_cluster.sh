#!/bin/bash
# Spark 클러스터 종료 스크립트
# 사용법: bash stop_cluster.sh

set -e

# 설정 변수 (실제 값으로 변경 필요)
MASTER_INSTANCE_ID="i-xxxxxxxxxxxxx"
WORKER1_INSTANCE_ID="i-xxxxxxxxxxxxx"
WORKER2_INSTANCE_ID="i-xxxxxxxxxxxxx"
MASTER_IP="<master-public-ip>"
SSH_KEY="~/.ssh/spark-cluster-key"
AWS_REGION="ap-northeast-2"

echo "========================================="
echo "Spark 클러스터 종료"
echo "========================================="

# 1. Spark 클러스터 종료
echo "[1/2] Spark 클러스터 종료 중..."
ssh -i ${SSH_KEY} ec2-user@${MASTER_IP} "/opt/spark/sbin/stop-all.sh" || true

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
