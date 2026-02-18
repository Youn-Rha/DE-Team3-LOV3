# DAG 실행 가이드

> Airflow 2.8.1 + SequentialExecutor + SQLite 환경 기준

---

## 사전 확인

Airflow EC2에 SSH 접속 후 아래가 실행 중인지 확인한다.

```bash
ps aux | grep -E "airflow (scheduler|webserver)" | grep -v grep
```

둘 다 없으면 시작:

```bash
source ~/airflow-venv/bin/activate
export AIRFLOW_HOME=~/airflow
airflow webserver -p 8080 -D
airflow scheduler -D
```

환경변수가 DAG에 반영되어야 하므로, **scheduler 실행 전** `~/.bashrc`에 설정 확인:

```bash
grep -E "MASTER_INSTANCE_ID|MASTER_PRIVATE_IP|S3_BUCKET|WORKER" ~/.bashrc
```

없으면 추가:

```bash
source ~/DE-Team3-LOV3/infra/env.sh
# 영구 적용
cat ~/DE-Team3-LOV3/infra/env.sh >> ~/.bashrc
```

---

## Airflow SSH Connection 확인

DAG의 `SSHOperator`가 Spark Master에 접속하기 위해 `spark_master` Connection이 등록되어 있어야 한다.

```bash
source ~/airflow-venv/bin/activate
airflow connections get spark_master 2>/dev/null | head -5
```

없으면 등록:

```bash
airflow connections add spark_master \
  --conn-type ssh \
  --conn-host 10.0.1.130 \
  --conn-login ec2-user \
  --conn-extra '{"key_file": "/home/ec2-user/.ssh/spark-cluster-key.pem", "no_host_key_check": true}'
```

---

## DAG 트리거 방법

> ⚠️ **`airflow dags trigger` CLI와 Web UI 트리거는 사용하지 않는다.**
>
> Airflow 2.8.1 버그로 인해 task instance가 생성되지 않고 DAG run이 즉시 success 처리된다.
> 반드시 아래 Python 스크립트를 사용한다.

### 기본 트리거 (특정 날짜)

```bash
source ~/airflow-venv/bin/activate
python3 ~/DE-Team3-LOV3/infra/airflow/trigger_dag.py 2026-02-12
```

날짜를 생략하면 **어제 날짜**로 자동 실행된다:

```bash
python3 ~/DE-Team3-LOV3/infra/airflow/trigger_dag.py
```

### 이전 실행이 남아있을 때 (재실행)

동일 날짜로 재실행하려면 기존 기록을 먼저 삭제한다:

```bash
source ~/airflow-venv/bin/activate
python3 << 'EOF'
from airflow import settings
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from sqlalchemy import text
from datetime import datetime, timezone

DAG_ID = "pothole_pipeline_spark_standalone"
EXEC_DATE = datetime(2026, 2, 12, tzinfo=timezone.utc)   # ← 날짜 변경

session = settings.Session()
try:
    session.execute(text(f"DELETE FROM task_instance WHERE dag_id='{DAG_ID}'"))
    session.execute(text(f"DELETE FROM dag_run WHERE dag_id='{DAG_ID}'"))
    session.commit()

    dag = SerializedDagModel.get_dag(dag_id=DAG_ID, session=session)
    dr = dag.create_dagrun(
        run_id=f"manual__{EXEC_DATE.isoformat()}",
        execution_date=EXEC_DATE,
        data_interval=(EXEC_DATE, EXEC_DATE),
        state=DagRunState.QUEUED,
        run_type=DagRunType.MANUAL,
        external_trigger=True,
        dag_hash=None,
        session=session,
    )
    session.commit()
    print(f"트리거 완료: {dr.run_id}")
except Exception as e:
    print(f"ERROR: {e}")
    session.rollback()
finally:
    session.close()
EOF
```

---

## 실행 상태 확인

```bash
source ~/airflow-venv/bin/activate
python3 << 'EOF'
from airflow import settings
from sqlalchemy import text
s = settings.Session()
runs = s.execute(text("SELECT state, start_date FROM dag_run WHERE dag_id='pothole_pipeline_spark_standalone' ORDER BY start_date DESC LIMIT 1")).fetchall()
print(f"DAG RUN: {runs[0][0] if runs else 'none'}")
tis = s.execute(text("SELECT task_id, state FROM task_instance WHERE dag_id='pothole_pipeline_spark_standalone' ORDER BY task_id")).fetchall()
for ti in tis:
    print(f"  {ti[0]}: {ti[1]}")
s.close()
EOF
```

또는 Web UI: `http://<AIRFLOW_EC2_PUBLIC_IP>:8080`

---

## 파이프라인 흐름 및 소요 시간

```
check_s3_input     (~5초)   S3 입력 데이터 존재 확인
    ↓
start_cluster      (~2분)   EC2 3대 시작 + 30초 안정화 대기
    ↓
start_spark        (~30초)  Spark start-all.sh
    ↓
download_code      (~30초)  S3에서 코드 Master로 다운로드
    ↓
run_stage1         (~수십분) spark-submit Stage1 (Anomaly Detection)
    ↓
check_s3_stage1_out (~5초)  Stage1 출력 S3 확인
    ↓
run_stage2         (~수십분) spark-submit Stage2 (Spatial Clustering)
    ↓
check_s3_stage2_out (~5초)  Stage2 출력 S3 확인
    ↓
stop_spark         (~30초)  Spark stop-all.sh
    ↓
stop_cluster       (~10초)  EC2 3대 중지 (실패해도 반드시 실행)
```

`stop_cluster`는 `trigger_rule=all_done`이므로 중간에 실패해도 EC2가 자동 중지된다.

---

## 코드 변경 후 S3 재업로드

처리 코드(`stage1/`, `stage2/`)나 설정 파일을 변경한 경우 S3에 올려야 반영된다.

```bash
source ~/DE-Team3-LOV3/infra/env.sh
bash ~/DE-Team3-LOV3/infra/s3_deploy/upload_code_to_s3.sh
```

---

## 트러블슈팅

### DAG run이 task 없이 즉시 success 처리됨

`airflow dags trigger` CLI 또는 Web UI를 사용했을 때 발생하는 Airflow 2.8.1 버그다.
→ 위의 **Python 스크립트 트리거** 방법을 사용한다.

### task log 확인

```bash
# 예: run_stage1 attempt=1 로그
cat ~/airflow/logs/dag_id=pothole_pipeline_spark_standalone/\
run_id=manual__2026-02-12T00:00:00+00:00/task_id=run_stage1/attempt=1.log
```

### run_stage1 실패 시 주요 원인

| 오류 메시지 | 원인 | 해결 |
|------------|------|------|
| `설정 파일 없음: config_prod.yaml` | S3에 config 파일 미업로드 | `upload_code_to_s3.sh` 실행 |
| `No FileSystem for scheme "s3"` | config 경로가 `s3://`로 시작 | `s3a://`로 수정 후 재업로드 |
| `SSH operator error: exit status = 1` | Master EC2가 꺼져있거나 Spark 미시작 | `start_cluster` 태스크가 정상인지 확인 |
| `Connection refused` (SSH) | `spark_master` Connection 미등록 | 위 SSH Connection 등록 참고 |

### 스케줄러 재시작 후 DAG 인식 안 될 때

```bash
source ~/airflow-venv/bin/activate
airflow dags list | grep pothole
# 없으면 DAG 파일 경로 확인
ls ~/airflow/dags/pothole_pipeline_dag.py
```
