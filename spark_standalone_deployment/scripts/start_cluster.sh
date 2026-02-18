#!/bin/bash
# Spark 클러스터 시작 스크립트
# 사용법: bash start_cluster.sh

set -e

# 설정 변수 (실제 값으로 변경 필요)
MASTER_INSTANCE_ID="i-xxxxxxxxxxxxx"
WORKER1_INSTANCE_ID="i-xxxxxxxxxxxxx"
WORKER2_INSTANCE_ID="i-xxxxxxxxxxxxx"
MASTER_IP="<master-public-ip>"
SSH_KEY="~/.ssh/spark-cluster-key"
AWS_REGION="ap-northeast-2"

echo "========================================="
echo "Spark 클러스터 시작"
echo "========================================="

# 1. EC2 인스턴스 시작
echo "[1/3] EC2 인스턴스 시작 중..."
aws ec2 start-instances \
    --instance-ids ${MASTER_INSTANCE_ID} ${WORKER1_INSTANCE_ID} ${WORKER2_INSTANCE_ID} \
    --region ${AWS_REGION}

# 2. 인스턴스가 running 상태가 될 때까지 대기
echo "[2/3] 인스턴스 시작 대기 중..."
aws ec2 wait instance-running \
    --instance-ids ${MASTER_INSTANCE_ID} ${WORKER1_INSTANCE_ID} ${WORKER2_INSTANCE_ID} \
    --region ${AWS_REGION}

echo "모든 인스턴스가 시작되었습니다. 30초 대기 중..."
sleep 30

# 3. Spark 클러스터 시작
echo "[3/3] Spark 클러스터 시작 중..."
ssh -i ${SSH_KEY} ec2-user@${MASTER_IP} "/opt/spark/sbin/start-all.sh"

echo "========================================="
echo "클러스터 시작 완료!"
echo "========================================="
echo ""
echo "Spark Master UI: http://${MASTER_IP}:8080"
echo ""
echo "상태 확인:"
echo "  ssh -i ${SSH_KEY} ec2-user@${MASTER_IP} 'jps'"
