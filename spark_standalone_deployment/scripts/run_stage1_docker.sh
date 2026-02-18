#!/bin/bash
# Docker로 Stage 1 처리 실행 스크립트
# 사용법: bash run_stage1_docker.sh [BATCH_DATE]
# 예시: bash run_stage1_docker.sh 2026-02-15

set -e

# 인자 확인
BATCH_DATE=${1:-$(date -d "yesterday" +%Y-%m-%d)}
MASTER_PRIVATE_IP="172.31.10.10"  # 실제 Master Private IP로 변경

echo "========================================="
echo "Stage 1 Anomaly Detection 실행"
echo "배치 날짜: ${BATCH_DATE}"
echo "========================================="

docker run --rm \
  --network host \
  -e AWS_DEFAULT_REGION=ap-northeast-2 \
  -e BATCH_DATE=${BATCH_DATE} \
  traffic-processing:latest \
  spark-submit \
    --master spark://${MASTER_PRIVATE_IP}:7077 \
    --deploy-mode client \
    --executor-memory 4g \
    --executor-cores 2 \
    --total-executor-cores 4 \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider \
    --conf spark.hadoop.fs.s3a.endpoint=s3.ap-northeast-2.amazonaws.com \
    /app/processing_service/stage1/stage1_anomaly_detection.py \
    --env stage1 \
    --batch-date ${BATCH_DATE}

echo "========================================="
echo "실행 완료!"
echo "========================================="
