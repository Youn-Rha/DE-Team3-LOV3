#!/bin/bash
# ============================================================
# SSH 키 설정 스크립트
# 대상: EC2-2 (Spark Master) 에서 실행
# Master → Worker1, Worker2 로 패스워드 없이 SSH 접속 설정
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../../env.sh" 2>/dev/null || {
    echo "ERROR: env.sh 파일을 찾을 수 없습니다."
    echo "  cp infra/env.sh.template infra/env.sh 후 값을 입력하세요."
    exit 1
}

echo "========================================="
echo "SSH 키 설정 (Master → Workers)"
echo "========================================="

# 1. SSH 키 생성 (없는 경우만)
if [ ! -f ~/.ssh/id_rsa ]; then
    echo "[1/3] SSH 키 생성 중..."
    ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
else
    echo "[1/3] SSH 키 이미 존재 - 스킵"
fi

# 2. SSH config 설정 (StrictHostKeyChecking 비활성화)
echo "[2/3] SSH config 설정 중..."
cat > ~/.ssh/config << EOF
Host *
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    LogLevel ERROR
EOF
chmod 600 ~/.ssh/config

# 3. 각 Worker에 public key 배포
echo "[3/3] Worker 노드에 SSH 키 배포 중..."
PUB_KEY=$(cat ~/.ssh/id_rsa.pub)

for WORKER_IP in "${WORKER1_PRIVATE_IP}" "${WORKER2_PRIVATE_IP}"; do
    echo "  → ${WORKER_IP} 에 키 배포 중..."
    # SSH_KEY_PATH가 설정된 경우 (초기 접속용 PEM 키 사용)
    ssh -i "${SSH_KEY_PATH}" -o StrictHostKeyChecking=no "${SSH_USER}@${WORKER_IP}" \
        "mkdir -p ~/.ssh && echo '${PUB_KEY}' >> ~/.ssh/authorized_keys && chmod 700 ~/.ssh && chmod 600 ~/.ssh/authorized_keys && sort -u ~/.ssh/authorized_keys -o ~/.ssh/authorized_keys"
    echo "    완료"
done

# 4. localhost에도 SSH 키 설정 (Master 자체에서 start-all.sh 실행 시 필요)
echo "${PUB_KEY}" >> ~/.ssh/authorized_keys
sort -u ~/.ssh/authorized_keys -o ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

echo "========================================="
echo "SSH 키 설정 완료!"
echo "========================================="
echo ""
echo "검증:"
echo "  ssh ${SSH_USER}@${WORKER1_PRIVATE_IP} 'hostname'"
echo "  ssh ${SSH_USER}@${WORKER2_PRIVATE_IP} 'hostname'"
