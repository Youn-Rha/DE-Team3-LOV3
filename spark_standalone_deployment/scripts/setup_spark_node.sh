#!/bin/bash
# Spark 노드 초기 설정 스크립트
# 사용법: bash setup_spark_node.sh

set -e

echo "========================================="
echo "Spark 노드 초기 설정 시작"
echo "========================================="

# 1. 시스템 업데이트
echo "[1/6] 시스템 업데이트 중..."
sudo yum update -y

# 2. Java 설치
echo "[2/6] Java 11 설치 중..."
sudo yum install -y java-11-amazon-corretto
java -version

# 3. Spark 다운로드 및 설치
echo "[3/6] Spark 다운로드 및 설치 중..."
cd /opt
sudo wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
sudo tar -xzf spark-3.5.0-bin-hadoop3.tgz
sudo mv spark-3.5.0-bin-hadoop3 spark
sudo chown -R ec2-user:ec2-user /opt/spark
sudo rm spark-3.5.0-bin-hadoop3.tgz

# 4. 환경 변수 설정
echo "[4/6] 환경 변수 설정 중..."
cat >> ~/.bashrc << 'EOF'

# Spark 환경 변수
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto
export PYSPARK_PYTHON=python3
EOF

source ~/.bashrc

# 5. S3 접근용 JAR 파일 다운로드
echo "[5/6] S3 접근용 JAR 파일 다운로드 중..."
cd /opt/spark/jars
sudo wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
sudo wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 6. Python3 및 pip 설치
echo "[6/6] Python3 및 pip 설치 중..."
sudo yum install -y python3-pip
pip3 install --user pyspark==3.5.0 pandas==2.0.3 numpy==1.24.3 pyyaml==6.0.1

echo "========================================="
echo "Spark 노드 설정 완료!"
echo "========================================="
echo ""
echo "다음 단계:"
echo "1. Spark 설정 파일 생성 (spark-env.sh, spark-defaults.conf)"
echo "2. Master 노드: workers 파일 생성"
echo "3. SSH 키 설정 (Master → Worker)"
echo ""
echo "설치 확인:"
echo "  - Java: java -version"
echo "  - Spark: /opt/spark/bin/spark-shell --version"
echo "  - Docker: docker --version"
