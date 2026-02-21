# Pothole Pipeline DAG 운영 가이드

> 이 문서는 Airflow_service EC2에서 포트홀 탐지 파이프라인을 트리거하고 운영하는 방법을 설명합니다.

---

## 목차
1. [디렉토리 구조](#1-디렉토리-구조)
2. [환경 전제 조건](#2-환경-전제-조건)
3. [코드 S3 업로드](#3-코드-s3-업로드)
4. [DAG 트리거](#4-dag-트리거)
5. [파이프라인 흐름 및 소요 시간](#5-파이프라인-흐름-및-소요-시간)
6. [상태 확인](#6-상태-확인)
7. [새 Stage 추가 방법](#7-새-stage-추가-방법)
8. [트러블슈팅](#8-트러블슈팅)

---

## 1. 디렉토리 구조

```
airflow_service/
├── dags/
│   ├── pothole_pipeline_dag.py   # 메인 DAG 정의
│   └── dag_utils.py               # spark-submit / S3 확인 명령 빌더
├── scripts/
│   └── trigger_dag.py             # DAG 트리거 스크립트 (Airflow 2.8.1 버그 우회)
└── DAG_TRIGGER_GUIDE.md          # 이 문서

# Airflow가 실제로 읽는 경로 (심링크)
~/airflow/dags/
├── pothole_pipeline_dag.py -> airflow_service/dags/pothole_pipeline_dag.py
└── dag_utils.py            -> airflow_service/dags/dag_utils.py
```

---

## 2. 환경 전제 조건

### 2-1. Airflow 서비스 실행 확인

```bash
# webserver, scheduler 실행 여부 확인
ps aux | grep "airflow"

# 실행 안 되어 있으면
source ~/airflow-venv/bin/activate
airflow webserver -D
airflow scheduler -D
```

### 2-2. 환경변수 확인 (~/.bashrc)

```bash
cat ~/.bashrc | grep -E "MASTER_|WORKER|S3_BUCKET|AWS_REGION|MASTER_PRIVATE"
```

필수 항목:
```bash
export MASTER_INSTANCE_ID="i-xxxxxxxxxxxxxxxxx"
export WORKER1_INSTANCE_ID="i-xxxxxxxxxxxxxxxxx"
export WORKER2_INSTANCE_ID="i-xxxxxxxxxxxxxxxxx"
export MASTER_PRIVATE_IP="10.0.1.xxx"
export AWS_REGION="ap-northeast-2"
export S3_BUCKET="softeer-7-de3-bucket"
export S3_CODE_PREFIX="spark-job-code"
```

### 2-3. Airflow SSH Connection 확인

```bash
source ~/airflow-venv/bin/activate
airflow connections get spark_master
```

없으면 등록:
```bash
airflow connections add spark_master \
    --conn-type ssh \
    --conn-host "${MASTER_PRIVATE_IP}" \
    --conn-login ec2-user \
    --conn-extra '{"key_file": "/home/ec2-user/.ssh/id_rsa"}'
```

---

## 3. 코드 S3 업로드

**DAG를 실행하기 전, processing_service 코드를 S3에 업로드해야 합니다.**
코드가 변경될 때마다 재업로드 필요합니다.

```bash
source ~/DE-Team3-LOV3/infra/env.sh
bash ~/DE-Team3-LOV3/infra/s3_deploy/upload_code_to_s3.sh
```

업로드 결과 확인:
```bash
aws s3 ls s3://softeer-7-de3-bucket/spark-job-code/stage1/ --region ap-northeast-2
aws s3 ls s3://softeer-7-de3-bucket/spark-job-code/stage2/ --region ap-northeast-2
```

예상 파일 목록:
- `stage1/`: `stage1_anomaly_detection.py`, `connection_stage1.py`, `config_stage1.yaml`
- `stage2/`: `stage2_spatial_clustering.py`, `connection_stage2.py`, `config_stage2.yaml`

---

## 4. DAG 트리거

### ⚠️ 중요: CLI / Web UI 사용 금지

Airflow 2.8.1 버그로 인해 `airflow dags trigger` CLI나 Web UI의 Trigger 버튼을 사용하면
task instance가 생성되지 않고 DAG이 즉시 `success` 처리됩니다.
**반드시 아래 Python 스크립트를 사용하세요.**

### 4-1. 수동 트리거

```bash
source ~/airflow-venv/bin/activate

# 전날 날짜로 자동 트리거
python3 ~/DE-Team3-LOV3/airflow_service/scripts/trigger_dag.py

# 특정 날짜 지정
python3 ~/DE-Team3-LOV3/airflow_service/scripts/trigger_dag.py 2026-02-12
```

### 4-2. 일배치 자동 트리거 (crontab)

매일 KST 10:00 (UTC 01:00)에 전날 데이터를 처리:

```bash
crontab -e
```

추가 내용:
```
0 1 * * * /home/ec2-user/airflow-venv/bin/python3 \
  /home/ec2-user/DE-Team3-LOV3/airflow_service/scripts/trigger_dag.py \
  >> /home/ec2-user/airflow/logs/cron_trigger.log 2>&1
```

---

## 5. 파이프라인 흐름 및 소요 시간

```
check_s3_input      (~5초)    — 입력 데이터 존재 확인
    ↓
start_cluster       (~2분)    — EC2 시작 + 30초 안정화 대기
    ↓
start_spark         (~30초)   — start-all.sh
    ↓
download_code       (~30초)   — S3 → Master /tmp/spark-job/
    ↓
run_stage1          (~5~20분) — Spark Anomaly Detection
    ↓
check_s3_stage1_out (~5초)
    ↓
run_stage2          (~5~15분) — Spark Spatial Clustering
    ↓
check_s3_stage2_out (~5초)
    ↓
stop_spark          (~30초)   — trigger_rule=all_done (성공/실패 무관)
    ↓
stop_cluster        (~10초)   — trigger_rule=all_done (비용 절감)
```

**Stage별 S3 입출력:**

| Stage | 입력 | 출력 |
|-------|------|------|
| Stage1 | `s3a://.../raw-sensor-data/dt={날짜}` | `s3a://.../stage1_anomaly_detected/dt={날짜}` |
| Stage2 | `s3a://.../stage1_anomaly_detected/dt={날짜}` | `s3a://.../stage2_spatial_clustering/dt={날짜}` |

---

## 6. 상태 확인

```bash
source ~/airflow-venv/bin/activate

# 최근 DAG run 상태
airflow dags list-runs -d pothole_pipeline_spark_standalone --limit 3

# task별 상태 (Python)
python3 << 'EOF'
from airflow import settings
from sqlalchemy import text
s = settings.Session()
tis = s.execute(text("""
    SELECT task_id, state, try_number
    FROM task_instance
    WHERE dag_id='pothole_pipeline_spark_standalone'
    ORDER BY task_id
""")).fetchall()
for ti in tis:
    print(f"  {ti[0]}: {ti[1]} (try={ti[2]})")
s.close()
EOF
```

로그 확인 (예: run_stage1):
```bash
tail -f ~/airflow/logs/dag_id=pothole_pipeline_spark_standalone/\
run_id=<run_id>/task_id=run_stage1/attempt=1.log
```

---

## 7. 새 Stage 추가 방법

Stage3 등 새로운 Spark job 추가 시:

**Step 1.** `processing_service/stage3/` 에 코드 작성

**Step 2.** S3 업로드 (upload_code_to_s3.sh 가 모든 stageN 자동 업로드)
```bash
bash ~/DE-Team3-LOV3/infra/s3_deploy/upload_code_to_s3.sh
```

**Step 3.** `airflow_service/dags/pothole_pipeline_dag.py` 에 태스크 추가:
```python
run_stage3 = SSHOperator(
    task_id="run_stage3",
    ssh_conn_id=SSH_CONN_ID,
    command=build_spark_submit_cmd(
        spark_master_uri=SPARK_MASTER_URI,
        job_dir=JOB_DIR,
        stage="stage3",
        main_script="stage3_xxx.py",
        py_files=["connection_stage3.py"],
        stage_args="--env stage3 --batch-date {{ ds }}",
        aws_region=AWS_REGION,
    ),
    cmd_timeout=3600,
)

check_s3_stage3_out = BashOperator(
    task_id="check_s3_stage3_out",
    bash_command=build_s3_check_cmd(
        S3_BUCKET, "stage3_output_prefix", AWS_REGION, partition="dt={{ ds }}"
    ),
)

# 의존성 업데이트
check_s3_stage2_out >> run_stage3 >> check_s3_stage3_out
check_s3_stage3_out >> stop_spark >> stop_cluster
```

---

## 8. 트러블슈팅

| 증상 | 원인 | 해결 |
|------|------|------|
| DAG run이 즉시 success (task 없이) | Airflow 2.8.1 dag_hash 버그 | `trigger_dag.py` 스크립트로 트리거 |
| `설정 파일 없음: config_stage1.yaml` | S3에 코드 미업로드 | `upload_code_to_s3.sh` 실행 |
| `connection_stage1 not found` | S3에 이전 파일명(connection.py) 등 잘못된 파일 | `upload_code_to_s3.sh` 재실행 후 DAG 재트리거 |
| `No FileSystem for scheme "s3"` | config에서 `s3://` 사용 | config 파일에서 `s3a://` 로 변경 |
| `Failed to connect to ...<port>` (Executor crash) | Spark Driver 포트가 보안 그룹에 차단됨 | `sg_spark` SG에 자기 자신으로부터 TCP All 허용 규칙 추가 |
| `Initial job has not accepted any resources` | Executor → Driver 연결 불가 (보안 그룹) | 위와 동일 |
| `SSH operator error: exit status = 1` | Master EC2 꺼짐 또는 Spark 미시작 | `start_cluster`, `start_spark` 태스크 로그 확인 |
| `입력 데이터 없음: .../dt=YYYY-MM-DD` | 해당 날짜 raw 데이터 없음 | S3에서 데이터 확인 후 다른 날짜로 트리거 |
| UNIQUE constraint (재트리거 오류) | 동일 run_id 이미 존재 | `trigger_dag.py` 는 자동 스킵. `airflow dags delete-dagrun` 으로 삭제 후 재트리거 |

### 보안 그룹 설정 (sg_spark 필수 inbound 규칙)

| 포트 | 소스 | 용도 |
|------|------|------|
| 7077 | sg_spark | Spark Master RPC |
| 7078 | sg_spark | Spark Worker |
| TCP 0-65535 | sg_spark | Driver ephemeral 포트 (클러스터 내부 통신) |
| 8080 | 0.0.0.0/0 | Spark Master Web UI |
| 22 | 0.0.0.0/0 | SSH |

> Driver 포트 규칙이 없으면 Executor가 Driver에 연결하지 못해
> `Initial job has not accepted any resources` 오류가 반복됩니다.
