#!/bin/bash
# ============================================================
# processing_service 코드를 S3에 업로드
# spark-submit에서 사용할 코드 + config 파일
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../env.sh" 2>/dev/null || {
    echo "ERROR: env.sh 파일을 찾을 수 없습니다."
    exit 1
}

PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
S3_DEST="s3://${S3_BUCKET}/${S3_CODE_PREFIX}"

echo "========================================="
echo "S3 코드 업로드"
echo "========================================="

# 1. Stage1 코드 업로드
echo "[1/2] Stage1 코드 업로드 중..."
aws s3 sync "${PROJECT_ROOT}/processing_service/stage1/" "${S3_DEST}/stage1/" \
    --exclude "__pycache__/*" \
    --exclude "*.pyc" \
    --exclude "data/*" \
    --region "${AWS_REGION}"
echo "  Stage1 업로드 완료: ${S3_DEST}/stage1/"

# 2. Stage2 코드 업로드
echo "[2/2] Stage2 코드 업로드 중..."
aws s3 sync "${PROJECT_ROOT}/processing_service/stage2/" "${S3_DEST}/stage2/" \
    --exclude "__pycache__/*" \
    --exclude "*.pyc" \
    --exclude "data/*" \
    --region "${AWS_REGION}"
echo "  Stage2 업로드 완료: ${S3_DEST}/stage2/"

echo "========================================="
echo "업로드 완료!"
echo "========================================="
echo ""
echo "확인:"
echo "  aws s3 ls ${S3_DEST}/stage1/ --region ${AWS_REGION}"
echo "  aws s3 ls ${S3_DEST}/stage2/ --region ${AWS_REGION}"
