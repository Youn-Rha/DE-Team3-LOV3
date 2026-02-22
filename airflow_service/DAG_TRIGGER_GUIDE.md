# 포트홀 파이프라인 운영 가이드

## 파이프라인 흐름

```
check_s3_input → start_cluster → start_spark → download_code → install_deps
  → run_stage1 → check_s3_stage1_out → run_stage2 → check_s3_stage2_out
  → load_to_rdb → refresh_views → send_email_report
  → stop_spark → stop_cluster
```

| 태스크 | 설명 | 실행 위치 |
|--------|------|-----------|
| check_s3_input | S3 raw-sensor-data 존재 확인 | Airflow EC2 |
| start_cluster | EC2 Master+Worker4 시작 | Airflow EC2 |
| start_spark | workers 파일 갱신 + start-all.sh | Spark Master |
| download_code | S3→Master 코드 다운로드 | Spark Master |
| install_deps | pyarrow 설치 (Master+Workers) | Spark Master |
| run_stage1 | Stage1 Anomaly Detection | Spark Master |
| run_stage2 | Stage2 Spatial Clustering | Spark Master |
| load_to_rdb | S3 Parquet → PostgreSQL 적재 | Serving EC2 |
| refresh_views | Materialized View 3개 갱신 | Serving EC2 |
| send_email_report | 일일 이메일 리포트 전송 | Serving EC2 |
| stop_spark/cluster | Spark 종료 + EC2 중지 (항상 실행) | Spark Master / Airflow EC2 |

## 스케줄

- **자동**: 매일 02:00 KST (`0 17 * * *` UTC)
- **수동**: `run_pipeline.sh` 또는 `trigger_dag.py`

## 실행 방법

```bash
# 어제 날짜 자동
bash ~/DE-Team3-LOV3/airflow_service/scripts/run_pipeline.sh

# 특정 날짜
bash ~/DE-Team3-LOV3/airflow_service/scripts/run_pipeline.sh 2026-02-12

# 같은 날짜 재실행 (기존 run 삭제)
bash ~/DE-Team3-LOV3/airflow_service/scripts/run_pipeline.sh 2026-02-12 --force
```

## 환경변수

### Airflow EC2 (`~/.bashrc` → `source infra/env.sh`)

| 변수 | 용도 |
|------|------|
| MASTER_INSTANCE_ID | EC2 시작/중지 |
| WORKER1~4_INSTANCE_ID | EC2 시작/중지 |
| MASTER_PRIVATE_IP | Spark master 주소 |
| WORKER1~4_PRIVATE_IP | Spark workers 파일 |
| AWS_REGION | AWS 리전 |
| S3_BUCKET | S3 버킷명 |
| SLACK_WEBHOOK_URL | Slack 장애 알림 (선택) |

### Serving EC2 (`.env` 파일)

| 변수 | 용도 |
|------|------|
| POSTGRES_PASSWORD | PostgreSQL 비밀번호 |
| GMAIL_ADDRESS | 이메일 발신 계정 |
| GMAIL_PASSWORD | Gmail 앱 비밀번호 |
| REPORT_EMAIL | 리포트 수신 이메일 |
| KAKAO_REST_API_KEY | 역지오코딩 (선택) |

## Airflow 커넥션

| Connection ID | Type | Host | 용도 |
|---------------|------|------|------|
| spark_master | SSH | 10.0.1.130 | Spark 클러스터 |
| serving_server | SSH | 10.0.1.160 | RDB/이메일 |

커넥션 추가:
```bash
source ~/airflow-venv/bin/activate
airflow connections add serving_server \
    --conn-type ssh \
    --conn-host 10.0.1.160 \
    --conn-login ec2-user \
    --conn-extra '{"key_file": "~/.ssh/id_rsa"}'
```

## 장애 알림

Task 실패 시 Slack Incoming Webhook으로 자동 알림.
- `SLACK_WEBHOOK_URL` 미설정 시 비활성
- 알림 내용: DAG명, 실패 Task, 날짜, 에러 메시지

## 트러블슈팅

| 증상 | 원인 | 해결 |
|------|------|------|
| DAG 트리거 후 task 미실행 | Airflow 2.8.1 버그 (paused 상태) | `run_pipeline.sh` 사용 (자동 unpause) |
| start_cluster 실패 | 환경변수 누락 | scheduler 재시작 (`~/.bashrc` 확인) |
| workers 2개만 뜸 | scheduler에 WORKER3/4 IP 미로드 | scheduler 재시작 |
| pyarrow 에러 | install_deps 실패 | Spark 노드 SSH 확인 |
| load_to_rdb 실패 | serving EC2 SSH 불가 | 보안그룹 22포트, SSH 키 확인 |
| email 전송 실패 | Gmail 인증 | `.env`의 GMAIL_PASSWORD (앱 비밀번호) 확인 |
| MV 갱신 실패 | Docker 미실행 | `docker-compose up -d` (serving EC2) |
