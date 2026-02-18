"""
Spark Standalone 클러스터용 Airflow DAG
교통 데이터 처리 파이프라인
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# 기본 설정
default_args = {
    'owner': 'softeer-DE3',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'spark_standalone_traffic_processing',
    default_args=default_args,
    description='Spark Standalone 클러스터로 교통 데이터 처리',
    schedule_interval=None,  # 수동 실행만 (자동 실행 안 함)
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,  # 동시 실행 1개로 제한
    tags=['spark', 'traffic', 'pothole'],
)

# 설정 변수
MASTER_INSTANCE_ID = "i-0786ec0b7fdaf6597"  # TODO: Master EC2 Instance ID로 변경
WORKER1_INSTANCE_ID = "i-0542358d86122c278"  # TODO: Worker 1 EC2 Instance ID로 변경
WORKER2_INSTANCE_ID = "i-0e6b59258b5c3bc5c"  # TODO: Worker 2 EC2 Instance ID로 변경
MASTER_PRIVATE_IP = "10.0.1.41"
AWS_REGION = "ap-northeast-2"

# Task 1: EC2 인스턴스 시작 (사용 안 함 - 주석 처리)
# start_ec2_instances = BashOperator(
#     task_id='start_ec2_instances',
#     bash_command=f"""
#     echo "EC2 인스턴스 시작 중..."
#     aws ec2 start-instances \
#         --instance-ids {MASTER_INSTANCE_ID} {WORKER1_INSTANCE_ID} {WORKER2_INSTANCE_ID} \
#         --region {AWS_REGION}
#     
#     echo "인스턴스 시작 대기 중..."
#     aws ec2 wait instance-running \
#         --instance-ids {MASTER_INSTANCE_ID} {WORKER1_INSTANCE_ID} {WORKER2_INSTANCE_ID} \
#         --region {AWS_REGION}
#     
#     echo "30초 대기..."
#     sleep 30
#     """,
#     dag=dag,
# )

# Task 2: Spark 클러스터 시작
start_spark_cluster = SSHOperator(
    task_id='start_spark_cluster',
    ssh_conn_id='spark_master',
    command='bash -c "/opt/spark/sbin/start-all.sh"',
    dag=dag,
)

# Task 3: Stage 1 - Anomaly Detection 실행
run_stage1_anomaly_detection = SSHOperator(
    task_id='run_stage1_anomaly_detection',
    ssh_conn_id='spark_master',
    command=f"""
    # Spark 작업 실행 (Docker 없이 직접 실행)
    # 스크립트 경로: /tmp/spark-job/stage1/
    /opt/spark/bin/spark-submit \
      --master spark://{MASTER_PRIVATE_IP}:7077 \
      --deploy-mode client \
      --executor-memory 4g \
      --executor-cores 2 \
      --total-executor-cores 4 \
      --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
      --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider \
      --conf spark.hadoop.fs.s3a.endpoint=s3.{AWS_REGION}.amazonaws.com \
      --conf spark.sql.files.maxPartitionBytes=134217728 \
      --conf spark.sql.files.openCostInBytes=134217728 \
      --conf spark.sql.adaptive.coalescePartitions.enabled=true \
      --conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=200 \
      --conf spark.sql.adaptive.enabled=true \
      --conf spark.hadoop.fs.s3a.connection.maximum=200 \
      --conf spark.hadoop.fs.s3a.threads.max=100 \
      --conf spark.hadoop.fs.s3a.block.size=134217728 \
      --py-files /tmp/spark-job/stage1/connection.py \
      /tmp/spark-job/stage1/stage1_anomaly_detection.py \
      --env stage1 \
      --batch-date {{{{ ds }}}}
    """,
    cmd_timeout=7200,  # 120분 타임아웃 (50,000개 파일 처리용)
    dag=dag,
)

# Task 4: Spark 클러스터 종료
stop_spark_cluster = SSHOperator(
    task_id='stop_spark_cluster',
    ssh_conn_id='spark_master',
    command='bash -c "/opt/spark/sbin/stop-all.sh"',
    trigger_rule='all_done',  # 성공/실패 관계없이 실행
    dag=dag,
)

# Task 5: EC2 인스턴스 중지 (사용 안 함 - 주석 처리)
# stop_ec2_instances = BashOperator(
#     task_id='stop_ec2_instances',
#     bash_command=f"""
#     echo "Spark 종료 후 5초 대기..."
#     sleep 5
#     
#     echo "EC2 인스턴스 중지 중..."
#     aws ec2 stop-instances \
#         --instance-ids {MASTER_INSTANCE_ID} {WORKER1_INSTANCE_ID} {WORKER2_INSTANCE_ID} \
#         --region {AWS_REGION}
#     
#     echo "EC2 인스턴스 중지 완료 (비용 절감)"
#     """,
#     trigger_rule='all_done',  # 성공/실패 관계없이 실행
#     dag=dag,
# )

# Task 의존성 정의
# 현재 설정: 클러스터 수동 관리, 작업만 제출
run_stage1_anomaly_detection

# 옵션: Spark 클러스터 자동 시작/종료
# start_spark_cluster >> run_stage1_anomaly_detection >> stop_spark_cluster

# 옵션: EC2 + Spark 클러스터 자동 시작/종료 (최대 비용 절감)
# start_ec2_instances >> start_spark_cluster >> run_stage1_anomaly_detection >> stop_spark_cluster >> stop_ec2_instances
