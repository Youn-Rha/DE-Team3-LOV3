#!/bin/bash
# Docker 이미지 빌드 및 Spark 클러스터 노드에 배포
# 사용법: bash build_and_deploy_docker.sh

set -e

# 설정 변수 (실제 값으로 변경 필요)
IMAGE_NAME="traffic-processing"
VERSION="latest"
MASTER_IP="<master-public-ip>"
WORKER1_IP="<worker1-public-ip>"
WORKER2_IP="<worker2-public-ip>"
SSH_KEY="~/.ssh/spark-cluster-key"

echo "========================================="
echo "Docker 이미지 빌드 및 배포"
echo "========================================="

# 1. 프로젝트 루트로 이동
cd "$(dirname "$0")/../.."

# 2. Docker 이미지 빌드
echo "[1/5] Docker 이미지 빌드 중..."
docker build -t ${IMAGE_NAME}:${VERSION} -f spark_standalone_deployment/Dockerfile .

# 3. 이미지를 tar.gz로 저장
echo "[2/5] Docker 이미지를 tar.gz로 저장 중..."
docker save ${IMAGE_NAME}:${VERSION} | gzip > /tmp/${IMAGE_NAME}.tar.gz
echo "이미지 크기: $(du -h /tmp/${IMAGE_NAME}.tar.gz | cut -f1)"

# 4. Master 노드에 전송
echo "[3/5] Master 노드에 이미지 전송 중..."
scp -i ${SSH_KEY} /tmp/${IMAGE_NAME}.tar.gz ec2-user@${MASTER_IP}:/tmp/
ssh -i ${SSH_KEY} ec2-user@${MASTER_IP} "docker load < /tmp/${IMAGE_NAME}.tar.gz && rm /tmp/${IMAGE_NAME}.tar.gz"
echo "Master 노드 배포 완료"

# 5. Worker 노드들에 전송
echo "[4/5] Worker 노드 1에 이미지 전송 중..."
scp -i ${SSH_KEY} /tmp/${IMAGE_NAME}.tar.gz ec2-user@${WORKER1_IP}:/tmp/
ssh -i ${SSH_KEY} ec2-user@${WORKER1_IP} "docker load < /tmp/${IMAGE_NAME}.tar.gz && rm /tmp/${IMAGE_NAME}.tar.gz"
echo "Worker 1 배포 완료"

echo "[5/5] Worker 노드 2에 이미지 전송 중..."
scp -i ${SSH_KEY} /tmp/${IMAGE_NAME}.tar.gz ec2-user@${WORKER2_IP}:/tmp/
ssh -i ${SSH_KEY} ec2-user@${WORKER2_IP} "docker load < /tmp/${IMAGE_NAME}.tar.gz && rm /tmp/${IMAGE_NAME}.tar.gz"
echo "Worker 2 배포 완료"

# 6. 로컬 임시 파일 삭제
rm /tmp/${IMAGE_NAME}.tar.gz

echo "========================================="
echo "배포 완료!"
echo "========================================="
echo ""
echo "이미지 확인:"
echo "  ssh -i ${SSH_KEY} ec2-user@${MASTER_IP} 'docker images'"
