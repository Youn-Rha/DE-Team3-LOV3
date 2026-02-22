#!/usr/bin/env python3
"""
매일 전날 날짜로 DAG를 트리거하는 스크립트 (cron용)

사용법:
  python3 trigger_dag.py                  # 전날 날짜로 자동 트리거
  python3 trigger_dag.py 2026-02-12       # 특정 날짜로 트리거

crontab 설정 예시 (매일 KST 10:00 = UTC 01:00):
  0 1 * * * /home/ec2-user/airflow-venv/bin/python3 \
    /home/ec2-user/DE-Team3-LOV3/infra/airflow/trigger_dag.py \
    >> /home/ec2-user/airflow/logs/cron_trigger.log 2>&1

참고: Airflow 2.8.1 버그로 인해 airflow dags trigger CLI 대신 이 스크립트를 사용해야 한다.
      dag_hash=None으로 dag_run을 생성해야 verify_integrity()가 호출되어 task instance가 만들어진다.
"""
import os
import sys

os.environ.setdefault("AIRFLOW_HOME", "/home/ec2-user/airflow")

from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from airflow import settings
from datetime import datetime, timezone, timedelta

DAG_ID = "pothole_pipeline_spark_standalone"

# 실행 날짜 결정: 인자 있으면 해당 날짜, 없으면 전날(UTC)
if len(sys.argv) > 1:
    try:
        EXEC_DATE = datetime.strptime(sys.argv[1], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        print(f"날짜 형식 오류. 올바른 형식: YYYY-MM-DD (예: 2026-02-12)")
        sys.exit(1)
else:
    EXEC_DATE = (datetime.now(tz=timezone.utc) - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

print(f"[{datetime.now()}] DAG 트리거 시작: {DAG_ID} | 실행 날짜: {EXEC_DATE.date()}")

session = settings.Session()
try:
    dag = SerializedDagModel.get_dag(dag_id=DAG_ID, session=session)
    if dag is None:
        print(f"ERROR: DAG '{DAG_ID}'를 찾을 수 없다. Airflow scheduler가 실행 중인지 확인하라.")
        sys.exit(1)

    run_id = f"manual__{EXEC_DATE.isoformat()}"

    # 동일 run_id가 이미 존재하면 스킵
    from airflow.models import DagRun
    existing = session.query(DagRun).filter_by(dag_id=DAG_ID, run_id=run_id).first()
    if existing:
        print(f"이미 실행된 run_id: {run_id} (state={existing.state}). 스킵.")
        sys.exit(0)

    dr = dag.create_dagrun(
        run_id=run_id,
        execution_date=EXEC_DATE,
        data_interval=(EXEC_DATE, EXEC_DATE),
        state=DagRunState.QUEUED,
        run_type=DagRunType.MANUAL,
        external_trigger=True,
        dag_hash=None,   # Airflow 2.8.1 버그 우회: None이어야 task instance가 생성된다
        session=session,
    )
    session.commit()
    print(f"[{datetime.now()}] 트리거 완료: {dr.run_id} | state={dr.state}")

except Exception as e:
    print(f"[{datetime.now()}] ERROR: {e}")
    session.rollback()
    sys.exit(1)
finally:
    session.close()
