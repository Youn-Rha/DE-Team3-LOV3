#!/bin/bash
# ============================================================
# Airflow 설치 스크립트 (EC2-1 에서 실행)
# Docker 미사용, Python venv로 직접 설치
# ============================================================

set -e

AIRFLOW_VERSION="2.8.1"
PYTHON_VERSION="$(python3 --version | cut -d ' ' -f2 | cut -d '.' -f1-2)"
AIRFLOW_HOME="${HOME}/airflow"
VENV_DIR="${HOME}/airflow-venv"
VENV_PIP="${VENV_DIR}/bin/pip"
VENV_AIRFLOW="${VENV_DIR}/bin/airflow"

echo "========================================="
echo "Airflow 설치 시작"
echo "========================================="

# 1. 시스템 패키지 설치
echo "[1/6] 시스템 패키지 설치 중..."
sudo yum update -y
sudo yum install -y python3-pip python3-devel gcc

# 2. Python venv 생성
echo "[2/6] Python venv 생성 중..."
python3 -m venv "${VENV_DIR}"

# 3. Airflow 설치 (venv 절대 경로 사용)
echo "[3/6] Airflow 설치 중..."
export AIRFLOW_HOME="${AIRFLOW_HOME}"
"${VENV_PIP}" install --upgrade pip

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
"${VENV_PIP}" install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
"${VENV_PIP}" install apache-airflow-providers-ssh
"${VENV_PIP}" install apache-airflow-providers-amazon
"${VENV_PIP}" install pyyaml boto3

# 4. Airflow DB 초기화
echo "[4/6] Airflow DB 초기화 중..."
"${VENV_AIRFLOW}" db init

# 5. Admin 유저 생성
echo "[5/6] Admin 유저 생성 중..."
"${VENV_AIRFLOW}" users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# 6. SSH Connection 설정 안내
echo "[6/6] SSH Connection 설정..."
echo ""
echo "Airflow Web UI 또는 CLI에서 다음 Connection을 추가하세요:"
echo ""
echo "  Connection Id: spark_master"
echo "  Connection Type: SSH"
echo "  Host: <SPARK_MASTER_PRIVATE_IP>"
echo "  Username: ec2-user"
echo "  Extra: {\"key_file\": \"<SSH_KEY_PATH>\"}"
echo ""

# bashrc에 Airflow 환경 추가
if ! grep -q "AIRFLOW_HOME" ~/.bashrc; then
    cat >> ~/.bashrc << EOF

# Airflow 환경
export AIRFLOW_HOME=${AIRFLOW_HOME}
alias airflow-activate='source ~/airflow-venv/bin/activate'
EOF
fi

echo "========================================="
echo "Airflow 설치 완료!"
echo "========================================="
echo ""
echo "시작 방법:"
echo "  source ~/airflow-venv/bin/activate"
echo "  airflow webserver -p 8080 -D"
echo "  airflow scheduler -D"
echo ""
echo "Web UI: http://<EC2-1-IP>:8080"
echo "로그인: admin / admin"
