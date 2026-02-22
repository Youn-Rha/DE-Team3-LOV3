# GitHub Actions CD — Spark 코드 자동 배포

## 문제

Spark 잡 코드(`processing_service/`)는 Airflow EC2가 아닌 **Spark Master EC2에서 실행**됩니다. 코드를 수정할 때마다 개발자가 직접 S3에 업로드하고, Spark Master에서 다운로드하는 과정을 수동으로 반복하면 배포 누락이나 버전 불일치가 발생할 수 있습니다.

```
개발자 PC → GitHub → ??? → S3 → Spark Master
                    ↑
               이 단계를 자동화
```

## 설계: GitHub Actions + S3 경유 배포

### 배포 흐름

```
1. 개발자가 processing_service/ 코드 수정 후 main에 push
2. GitHub Actions가 자동으로 S3에 코드 sync
3. DAG 실행 시 Spark Master가 S3에서 최신 코드 다운로드
```

Spark Master에 직접 배포하지 않고 **S3를 중간 저장소로 사용**합니다. DAG의 `download_code` 태스크가 매 실행마다 S3에서 최신 코드를 가져오므로, S3에만 올리면 다음 파이프라인 실행부터 자동 반영됩니다.

### GitHub Actions Workflow

```yaml
name: Deploy processing_service to S3

on:
  push:
    branches: [main]
    paths:
      - 'processing_service/**'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ap-northeast-2

      - name: Sync Stage1 to S3
        run: |
          aws s3 sync processing_service/stage1/ \
            s3://${{ secrets.S3_BUCKET }}/spark-job-code/stage1/ \
            --exclude "__pycache__/*" --exclude "*.pyc" --delete

      - name: Sync Stage2 to S3
        run: |
          aws s3 sync processing_service/stage2/ \
            s3://${{ secrets.S3_BUCKET }}/spark-job-code/stage2/ \
            --exclude "__pycache__/*" --exclude "*.pyc" --delete
```

### 트리거 조건

- **브랜치**: `main`에 push될 때만 실행 (PR 머지 시점)
- **경로 필터**: `processing_service/**` 하위 파일이 변경된 경우에만 실행
- 다른 서비스(`serving_service/`, `airflow_service/` 등)만 수정하면 워크플로우가 실행되지 않아 불필요한 배포를 방지

### S3 Sync 옵션

| 옵션 | 역할 |
|------|------|
| `--exclude "__pycache__/*"` | 바이트코드 캐시 제외 |
| `--exclude "*.pyc"` | 컴파일된 파이썬 파일 제외 |
| `--delete` | S3에만 있고 로컬에 없는 파일 삭제 (코드 삭제 시 반영) |

`--delete` 옵션으로 **S3의 코드가 항상 GitHub main 브랜치와 동일한 상태**를 유지합니다.

### DAG에서의 코드 다운로드

GitHub Actions가 S3에 올린 코드는 DAG의 `infra_setup.download_code` 태스크에서 Spark Master로 가져옵니다.

```
aws s3 cp s3://{bucket}/spark-job-code/stage1/ /tmp/spark-job/stage1/ --recursive
aws s3 cp s3://{bucket}/spark-job-code/stage2/ /tmp/spark-job/stage2/ --recursive
```

매 실행마다 `/tmp/spark-job/`을 초기화하고 S3에서 새로 다운로드하므로, 이전 실행의 코드가 남아있는 문제가 발생하지 않습니다.

## 수동 배포 (대안)

GitHub Actions 없이 로컬에서 직접 배포할 수도 있습니다.

```bash
bash infra/s3_deploy/upload_code_to_s3.sh
```

이 스크립트는 GitHub Actions와 동일하게 `aws s3 sync`를 실행합니다. 긴급 수정이 필요하거나 Actions 환경에 문제가 있을 때 사용합니다.

## GitHub Secrets 설정

| Secret | 설명 |
|--------|------|
| `AWS_ACCESS_KEY_ID` | S3 업로드용 IAM Access Key |
| `AWS_SECRET_ACCESS_KEY` | S3 업로드용 IAM Secret Key |
| `S3_BUCKET` | 데이터 S3 버킷명 |

IAM 사용자에게는 `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` 권한만 부여하여 최소 권한 원칙을 적용합니다.
