"""
Spark Standalone 클러스터 기반 포트홀 탐지 파이프라인 DAG

파이프라인 흐름:
  1. check_s3_input       — S3에 raw-sensor-data 존재 확인
  2. start_cluster        — EC2 시작 + Spark start-all.sh
  3. download_code        — S3에서 processing 코드를 Master로 다운로드
  4. run_stage1           — spark-submit Stage1 (Anomaly Detection)
  5. check_s3_stage1_out  — Stage1 출력 S3 존재 확인
  6. run_stage2           — spark-submit Stage2 (Spatial Clustering)
  7. check_s3_stage2_out  — Stage2 출력 S3 존재 확인
  8. stop_cluster         — Spark stop-all.sh + EC2 중지 (trigger_rule=all_done)
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# ============================================================
# 설정
# ============================================================
MASTER_INSTANCE_ID = "i-0786ec0b7fdaf6597"  # TODO: 실제 값으로 변경
WORKER1_INSTANCE_ID = "i-0542358d86122c278"  # TODO: 실제 값으로 변경
WORKER2_INSTANCE_ID = "i-0e6b59258b5c3bc5c"  # TODO: 실제 값으로 변경
MASTER_PRIVATE_IP = "10.0.1.41"  # TODO: 실제 값으로 변경
AWS_REGION = "ap-northeast-2"

S3_BUCKET = "softeer-final-porj"
S3_CODE_PREFIX = "spark-job-code"
S3_RAW_DATA_PREFIX = "raw-sensor-data"
S3_STAGE1_OUTPUT_PREFIX = "stage1_anomaly_detected"
S3_STAGE2_OUTPUT_PREFIX = "stage2_aggregated"

SPARK_MASTER_URI = f"spark://{MASTER_PRIVATE_IP}:7077"
JOB_DIR = "/tmp/spark-job"

SSH_CONN_ID = "spark_master"

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
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "pothole", "pipeline"],
) as dag:

    # --------------------------------------------------------
    # 1. S3 입력 데이터 존재 확인
    # --------------------------------------------------------
    check_s3_input = BashOperator(
        task_id="check_s3_input",
        bash_command=f"""
        echo "S3 입력 데이터 확인 중..."
        RESULT=$(aws s3 ls s3://{S3_BUCKET}/{S3_RAW_DATA_PREFIX}/dt={{{{ ds }}}}/ --region {AWS_REGION} 2>&1)
        if [ -z "$RESULT" ]; then
            echo "ERROR: S3에 입력 데이터가 없습니다: s3://{S3_BUCKET}/{S3_RAW_DATA_PREFIX}/dt={{{{ ds }}}}/"
            exit 1
        fi
        echo "입력 데이터 확인 완료"
        echo "$RESULT" | head -5
        """,
    )

    # --------------------------------------------------------
    # 2. EC2 인스턴스 시작 + Spark 클러스터 시작
    # --------------------------------------------------------
    start_cluster = BashOperator(
        task_id="start_cluster",
        bash_command=f"""
        echo "EC2 인스턴스 시작 중..."
        aws ec2 start-instances \
            --instance-ids {MASTER_INSTANCE_ID} {WORKER1_INSTANCE_ID} {WORKER2_INSTANCE_ID} \
            --region {AWS_REGION}

        echo "인스턴스 running 대기 중..."
        aws ec2 wait instance-running \
            --instance-ids {MASTER_INSTANCE_ID} {WORKER1_INSTANCE_ID} {WORKER2_INSTANCE_ID} \
            --region {AWS_REGION}

        echo "30초 네트워크 안정화 대기..."
        sleep 30
        """,
    )

    start_spark = SSHOperator(
        task_id="start_spark",
        ssh_conn_id=SSH_CONN_ID,
        command="source ~/.bashrc && /opt/spark/sbin/start-all.sh",
        cmd_timeout=120,
    )

    # --------------------------------------------------------
    # 3. S3에서 코드 다운로드
    # --------------------------------------------------------
    download_code = SSHOperator(
        task_id="download_code",
        ssh_conn_id=SSH_CONN_ID,
        command=f"""
        rm -rf {JOB_DIR} && mkdir -p {JOB_DIR}
        aws s3 cp s3://{S3_BUCKET}/{S3_CODE_PREFIX}/stage1/ {JOB_DIR}/stage1/ --recursive --region {AWS_REGION}
        aws s3 cp s3://{S3_BUCKET}/{S3_CODE_PREFIX}/stage2/ {JOB_DIR}/stage2/ --recursive --region {AWS_REGION}
        echo "코드 다운로드 완료"
        ls -la {JOB_DIR}/stage1/
        ls -la {JOB_DIR}/stage2/
        """,
        cmd_timeout=300,
    )

    # --------------------------------------------------------
    # 4. Stage1 - Anomaly Detection
    # --------------------------------------------------------
    run_stage1 = SSHOperator(
        task_id="run_stage1",
        ssh_conn_id=SSH_CONN_ID,
        command=f"""
        source ~/.bashrc
        /opt/spark/bin/spark-submit \
            --master {SPARK_MASTER_URI} \
            --deploy-mode client \
            --executor-memory 4g \
            --executor-cores 2 \
            --total-executor-cores 4 \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider \
            --conf spark.hadoop.fs.s3a.endpoint=s3.{AWS_REGION}.amazonaws.com \
            --conf spark.sql.adaptive.enabled=true \
            --conf spark.sql.adaptive.coalescePartitions.enabled=true \
            --conf spark.hadoop.fs.s3a.connection.maximum=200 \
            --conf spark.hadoop.fs.s3a.threads.max=100 \
            --py-files {JOB_DIR}/stage1/connection.py \
            {JOB_DIR}/stage1/stage1_anomaly_detection.py \
            --env prod \
            --batch-date {{{{ ds }}}}
        """,
        cmd_timeout=7200,
    )

    # --------------------------------------------------------
    # 5. Stage1 출력 확인
    # --------------------------------------------------------
    check_s3_stage1_out = BashOperator(
        task_id="check_s3_stage1_out",
        bash_command=f"""
        echo "Stage1 출력 확인 중..."
        RESULT=$(aws s3 ls s3://{S3_BUCKET}/{S3_STAGE1_OUTPUT_PREFIX}/dt={{{{ ds }}}}/ --region {AWS_REGION} 2>&1)
        if [ -z "$RESULT" ]; then
            echo "ERROR: Stage1 출력이 없습니다: s3://{S3_BUCKET}/{S3_STAGE1_OUTPUT_PREFIX}/dt={{{{ ds }}}}/"
            exit 1
        fi
        echo "Stage1 출력 확인 완료"
        echo "$RESULT" | head -5
        """,
    )

    # --------------------------------------------------------
    # 6. Stage2 - Spatial Clustering
    # --------------------------------------------------------
    run_stage2 = SSHOperator(
        task_id="run_stage2",
        ssh_conn_id=SSH_CONN_ID,
        command=f"""
        source ~/.bashrc
        /opt/spark/bin/spark-submit \
            --master {SPARK_MASTER_URI} \
            --deploy-mode client \
            --executor-memory 4g \
            --executor-cores 2 \
            --total-executor-cores 4 \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider \
            --conf spark.hadoop.fs.s3a.endpoint=s3.{AWS_REGION}.amazonaws.com \
            --conf spark.sql.adaptive.enabled=true \
            {JOB_DIR}/stage2/spatial_clustering.py \
            --config-path {JOB_DIR}/stage2/config_prod.yaml
        """,
        cmd_timeout=3600,
    )

    # --------------------------------------------------------
    # 7. Stage2 출력 확인
    # --------------------------------------------------------
    check_s3_stage2_out = BashOperator(
        task_id="check_s3_stage2_out",
        bash_command=f"""
        echo "Stage2 출력 확인 중..."
        RESULT=$(aws s3 ls s3://{S3_BUCKET}/{S3_STAGE2_OUTPUT_PREFIX}/ --region {AWS_REGION} 2>&1)
        if [ -z "$RESULT" ]; then
            echo "ERROR: Stage2 출력이 없습니다: s3://{S3_BUCKET}/{S3_STAGE2_OUTPUT_PREFIX}/"
            exit 1
        fi
        echo "Stage2 출력 확인 완료"
        echo "$RESULT" | head -5
        """,
    )

    # --------------------------------------------------------
    # 8. Spark 클러스터 종료 + EC2 중지
    # --------------------------------------------------------
    stop_spark = SSHOperator(
        task_id="stop_spark",
        ssh_conn_id=SSH_CONN_ID,
        command="source ~/.bashrc && /opt/spark/sbin/stop-all.sh",
        cmd_timeout=120,
        trigger_rule="all_done",
    )

    stop_cluster = BashOperator(
        task_id="stop_cluster",
        bash_command=f"""
        echo "EC2 인스턴스 중지 중..."
        sleep 5
        aws ec2 stop-instances \
            --instance-ids {MASTER_INSTANCE_ID} {WORKER1_INSTANCE_ID} {WORKER2_INSTANCE_ID} \
            --region {AWS_REGION}
        echo "EC2 인스턴스 중지 완료 (비용 절감)"
        """,
        trigger_rule="all_done",
    )

    # --------------------------------------------------------
    # 의존성 정의
    # --------------------------------------------------------
    check_s3_input >> start_cluster >> start_spark >> download_code
    download_code >> run_stage1 >> check_s3_stage1_out
    check_s3_stage1_out >> run_stage2 >> check_s3_stage2_out
    check_s3_stage2_out >> stop_spark >> stop_cluster
