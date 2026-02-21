"""
Spark Standalone 클러스터 기반 포트홀 탐지 파이프라인 DAG

파이프라인 흐름:
  1. check_s3_input       — S3에 raw-sensor-data 존재 확인
  2. start_cluster        — EC2 인스턴스 시작 (Master + Worker x4)
  3. start_spark          — Spark 클러스터 start-all.sh
  4. download_code        — S3 → Spark Master /tmp/spark-job/ 코드 다운로드
  5. install_deps         — Master + Worker 전체에 Python 패키지 설치 (pyarrow 등)
  6. run_stage1           — spark-submit: Stage1 Anomaly Detection
  7. check_s3_stage1_out  — Stage1 출력 S3 존재 확인
  8. run_stage2           — spark-submit: Stage2 Spatial Clustering
  9. check_s3_stage2_out  — Stage2 출력 S3 존재 확인
 10. stop_spark           — Spark stop-all.sh  (trigger_rule=all_done)
 11. stop_cluster         — EC2 인스턴스 중지  (trigger_rule=all_done)

환경변수 (Airflow EC2 ~/.bashrc 또는 Airflow 환경에 설정 필요):
  MASTER_INSTANCE_ID, WORKER1~4_INSTANCE_ID
  MASTER_PRIVATE_IP, AWS_REGION, S3_BUCKET

새 Stage 추가 방법:
  → airflow_service/dags/dag_utils.py 주석 참조
"""

import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

from dag_utils import build_spark_submit_cmd, build_s3_check_cmd

# ============================================================
# 설정 — Airflow EC2의 ~/.bashrc 에 환경변수로 등록
# ============================================================
MASTER_INSTANCE_ID  = os.environ.get("MASTER_INSTANCE_ID", "")
WORKER1_INSTANCE_ID = os.environ.get("WORKER1_INSTANCE_ID", "")
WORKER2_INSTANCE_ID = os.environ.get("WORKER2_INSTANCE_ID", "")
WORKER3_INSTANCE_ID = os.environ.get("WORKER3_INSTANCE_ID", "")
WORKER4_INSTANCE_ID = os.environ.get("WORKER4_INSTANCE_ID", "")
MASTER_PRIVATE_IP   = os.environ.get("MASTER_PRIVATE_IP", "")
WORKER1_PRIVATE_IP  = os.environ.get("WORKER1_PRIVATE_IP", "")
WORKER2_PRIVATE_IP  = os.environ.get("WORKER2_PRIVATE_IP", "")
WORKER3_PRIVATE_IP  = os.environ.get("WORKER3_PRIVATE_IP", "")
WORKER4_PRIVATE_IP  = os.environ.get("WORKER4_PRIVATE_IP", "")
AWS_REGION          = os.environ.get("AWS_REGION", "ap-northeast-2")

S3_BUCKET              = os.environ.get("S3_BUCKET", "")
S3_CODE_PREFIX         = os.environ.get("S3_CODE_PREFIX", "spark-job-code")
S3_RAW_DATA_PREFIX     = os.environ.get("S3_RAW_DATA_PREFIX", "raw-sensor-data")
S3_STAGE1_OUTPUT_PREFIX = os.environ.get("S3_STAGE1_OUTPUT_PREFIX", "stage1_anomaly_detected")
S3_STAGE2_OUTPUT_PREFIX = os.environ.get("S3_STAGE2_OUTPUT_PREFIX", "stage2_spatial_clustering")

SPARK_MASTER_URI = f"spark://{MASTER_PRIVATE_IP}:7077"
JOB_DIR          = "/tmp/spark-job"
SSH_CONN_ID      = "spark_master"

# ============================================================
# DAG 정의
# ============================================================
default_args = {
    "owner": "softeer-DE3",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="pothole_pipeline_spark_standalone",
    default_args=default_args,
    description="Spark Standalone 클러스터로 포트홀 탐지 파이프라인 (Stage1 + Stage2)",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "pothole", "pipeline"],
) as dag:

    # --------------------------------------------------------
    # 1. S3 입력 데이터 존재 확인
    # --------------------------------------------------------
    check_s3_input = BashOperator(
        task_id="check_s3_input",
        bash_command=build_s3_check_cmd(
            S3_BUCKET, S3_RAW_DATA_PREFIX, AWS_REGION, partition="dt={{ ds }}"
        ),
    )

    # --------------------------------------------------------
    # 2. EC2 인스턴스 시작
    # --------------------------------------------------------
    start_cluster = BashOperator(
        task_id="start_cluster",
        bash_command=f"""
        echo "EC2 인스턴스 시작 중..."
        aws ec2 start-instances \
            --instance-ids {MASTER_INSTANCE_ID} {WORKER1_INSTANCE_ID} {WORKER2_INSTANCE_ID} {WORKER3_INSTANCE_ID} {WORKER4_INSTANCE_ID} \
            --region {AWS_REGION}

        echo "인스턴스 running 대기 중..."
        aws ec2 wait instance-running \
            --instance-ids {MASTER_INSTANCE_ID} {WORKER1_INSTANCE_ID} {WORKER2_INSTANCE_ID} {WORKER3_INSTANCE_ID} {WORKER4_INSTANCE_ID} \
            --region {AWS_REGION}

        echo "30초 네트워크 안정화 대기..."
        sleep 30
        """,
    )

    # --------------------------------------------------------
    # 3. Spark 클러스터 시작
    # --------------------------------------------------------
    start_spark = SSHOperator(
        task_id="start_spark",
        ssh_conn_id=SSH_CONN_ID,
        command=f"""
        source ~/.bashrc

        echo "=== Spark workers 파일 갱신 ==="
        cat > /opt/spark/conf/workers <<EOF
{WORKER1_PRIVATE_IP}
{WORKER2_PRIVATE_IP}
{WORKER3_PRIVATE_IP}
{WORKER4_PRIVATE_IP}
EOF
        echo "workers 파일 내용:"
        cat /opt/spark/conf/workers

        /opt/spark/sbin/start-all.sh
        """,
        cmd_timeout=120,
    )

    # --------------------------------------------------------
    # 4. S3 → Master 코드 다운로드
    # --------------------------------------------------------
    download_code = SSHOperator(
        task_id="download_code",
        ssh_conn_id=SSH_CONN_ID,
        command=f"""
        rm -rf {JOB_DIR} && mkdir -p {JOB_DIR}
        aws s3 cp s3://{S3_BUCKET}/{S3_CODE_PREFIX}/stage1/ {JOB_DIR}/stage1/ --recursive --region {AWS_REGION}
        aws s3 cp s3://{S3_BUCKET}/{S3_CODE_PREFIX}/stage2/ {JOB_DIR}/stage2/ --recursive --region {AWS_REGION}
        echo "=== 다운로드 완료 ==="
        ls -la {JOB_DIR}/stage1/
        ls -la {JOB_DIR}/stage2/
        """,
        cmd_timeout=300,
    )

    # --------------------------------------------------------
    # 5. Python 의존성 설치 (Master + 모든 Worker)
    #    mapInPandas 등 PyArrow 기반 PySpark API 사용에 필요
    # --------------------------------------------------------
    install_deps = SSHOperator(
        task_id="install_deps",
        ssh_conn_id=SSH_CONN_ID,
        command=f"""
        echo "=== Master에 pyarrow 설치 ==="
        pip3 install pyarrow --quiet

        echo "=== Workers에 pyarrow 설치 ==="
        for worker in {WORKER1_PRIVATE_IP} {WORKER2_PRIVATE_IP} {WORKER3_PRIVATE_IP} {WORKER4_PRIVATE_IP}; do
            ssh -o StrictHostKeyChecking=no $worker "pip3 install pyarrow --quiet" &
        done
        wait
        echo "=== 의존성 설치 완료 ==="
        """,
        cmd_timeout=300,
    )

    # --------------------------------------------------------
    # 6. Stage1 — Anomaly Detection
    #    processing_service/stage1/stage1_anomaly_detection.py
    #    --env stage1  →  config_stage1.yaml 사용
    # --------------------------------------------------------
    run_stage1 = SSHOperator(
        task_id="run_stage1",
        ssh_conn_id=SSH_CONN_ID,
        command=build_spark_submit_cmd(
            spark_master_uri=SPARK_MASTER_URI,
            job_dir=JOB_DIR,
            stage="stage1",
            main_script="stage1_anomaly_detection.py",
            py_files=["connection_stage1.py"],
            stage_args="--env stage1 --batch-date {{ ds }}",
            aws_region=AWS_REGION,
            total_executor_cores=8,
        ),
        cmd_timeout=7200,
    )

    # --------------------------------------------------------
    # 6. Stage1 출력 확인
    # --------------------------------------------------------
    check_s3_stage1_out = BashOperator(
        task_id="check_s3_stage1_out",
        bash_command=build_s3_check_cmd(
            S3_BUCKET, S3_STAGE1_OUTPUT_PREFIX, AWS_REGION, partition="dt={{ ds }}"
        ),
    )

    # --------------------------------------------------------
    # 7. Stage2 — Spatial Clustering
    #    processing_service/stage2/stage2_spatial_clustering.py
    #    --env stage2  →  config_stage2.yaml 사용
    # --------------------------------------------------------
    run_stage2 = SSHOperator(
        task_id="run_stage2",
        ssh_conn_id=SSH_CONN_ID,
        command=build_spark_submit_cmd(
            spark_master_uri=SPARK_MASTER_URI,
            job_dir=JOB_DIR,
            stage="stage2",
            main_script="stage2_spatial_clustering.py",
            py_files=["connection_stage2.py"],
            stage_args="--env stage2 --batch-date {{ ds }}",
            aws_region=AWS_REGION,
            total_executor_cores=8,
        ),
        cmd_timeout=3600,
    )

    # --------------------------------------------------------
    # 8. Stage2 출력 확인
    # --------------------------------------------------------
    check_s3_stage2_out = BashOperator(
        task_id="check_s3_stage2_out",
        bash_command=build_s3_check_cmd(
            S3_BUCKET, S3_STAGE2_OUTPUT_PREFIX, AWS_REGION, partition="dt={{ ds }}"
        ),
    )

    # --------------------------------------------------------
    # 9-10. Spark + EC2 종료 (성공/실패 무관하게 항상 실행)
    # --------------------------------------------------------
    stop_spark = SSHOperator(
        task_id="stop_spark",
        ssh_conn_id=SSH_CONN_ID,
        command="source ~/.bashrc && /opt/spark/sbin/stop-all.sh ;",
        cmd_timeout=120,
        trigger_rule="all_done",
    )

    stop_cluster = BashOperator(
        task_id="stop_cluster",
        bash_command=f"""
        echo "EC2 인스턴스 중지 중..."
        sleep 5
        aws ec2 stop-instances \
            --instance-ids {MASTER_INSTANCE_ID} {WORKER1_INSTANCE_ID} {WORKER2_INSTANCE_ID} {WORKER3_INSTANCE_ID} {WORKER4_INSTANCE_ID} \
            --region {AWS_REGION}
        echo "EC2 인스턴스 중지 완료 (비용 절감)"
        """,
        trigger_rule="all_done",
    )

    # --------------------------------------------------------
    # 의존성
    # --------------------------------------------------------
    check_s3_input >> start_cluster >> start_spark >> download_code
    download_code >> install_deps >> run_stage1 >> check_s3_stage1_out
    check_s3_stage1_out >> run_stage2 >> check_s3_stage2_out
    check_s3_stage2_out >> stop_spark >> stop_cluster
