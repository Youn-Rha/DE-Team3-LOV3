#!/bin/bash
# ============================================================
# Spark 노드 초기 설정 스크립트
# 대상: EC2-2 (Master), EC2-3 (Worker1), EC2-4 (Worker2)
# 사용법: bash setup_spark_node.sh
# ============================================================

set -e

echo "========================================="
echo "Spark 노드 초기 설정 시작"
echo "========================================="

# 1. 시스템 업데이트
echo "[1/6] 시스템 업데이트 중..."
sudo yum update -y

# 2. Java 11 설치
echo "[2/6] Java 11 설치 중..."
sudo yum install -y java-11-amazon-corretto
java -version

# 3. Spark 3.5.0 다운로드 및 설치
echo "[3/6] Spark 다운로드 및 설치 중..."
if [ ! -d /opt/spark ]; then
    cd /opt
    sudo wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
    sudo tar -xzf spark-3.5.0-bin-hadoop3.tgz
    sudo mv spark-3.5.0-bin-hadoop3 spark
    sudo chown -R ec2-user:ec2-user /opt/spark
    sudo rm -f spark-3.5.0-bin-hadoop3.tgz
    echo "Spark 설치 완료"
else
    echo "Spark 이미 설치됨 - 스킵"
fi

# 필요한 디렉토리 생성
mkdir -p /opt/spark/logs /opt/spark/run /opt/spark/spark-events

# 4. 환경 변수 설정
echo "[4/6] 환경 변수 설정 중..."
if ! grep -q "SPARK_HOME" ~/.bashrc; then
    cat >> ~/.bashrc << 'EOF'

# Spark 환경 변수
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto
export PYSPARK_PYTHON=python3
EOF
    echo "환경 변수 추가 완료"
else
    echo "환경 변수 이미 설정됨 - 스킵"
fi

source ~/.bashrc

# 5. S3 접근용 JAR 파일 다운로드
echo "[5/6] S3 접근용 JAR 파일 다운로드 중..."
cd /opt/spark/jars
if [ ! -f hadoop-aws-3.3.4.jar ]; then
    sudo wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
    sudo wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
    echo "JAR 파일 다운로드 완료"
else
    echo "JAR 파일 이미 존재 - 스킵"
fi

# 6. Python3 및 필수 패키지 설치
echo "[6/6] Python3 및 pip 패키지 설치 중..."
sudo yum install -y python3-pip
pip3 install --user \
    pyspark==3.5.0 \
    pandas==2.0.3 \
    numpy==1.24.3 \
    pyyaml==6.0.1 \
    scipy \
    boto3

echo "========================================="
echo "Spark 노드 설정 완료!"
echo "========================================="
echo ""
echo "다음 단계:"
echo "  1. Master에서 setup_ssh_keys.sh 실행"
echo "  2. deploy_spark_configs.sh 실행"
echo ""
echo "설치 확인:"
echo "  - Java: java -version"
echo "  - Spark: /opt/spark/bin/spark-shell --version"
echo "  - Python: python3 --version"
