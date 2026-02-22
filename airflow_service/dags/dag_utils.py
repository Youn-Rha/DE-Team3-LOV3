"""
dag_utils.py — Airflow DAG 공통 유틸리티

spark-submit 명령과 S3 존재 확인 bash 명령을 생성하는 헬퍼 함수.

새 Stage 추가 방법:
  1. processing_service/stageN/ 에 코드 작성
  2. infra/s3_deploy/upload_code_to_s3.sh 실행 (모든 stageN 자동 업로드)
  3. pothole_pipeline_dag.py 에 아래 함수로 태스크 추가:
       - run_stageN   : SSHOperator(command=build_spark_submit_cmd(...))
       - check_s3_out : BashOperator(bash_command=build_s3_check_cmd(...))
"""


def build_spark_submit_cmd(
    spark_master_uri: str,
    job_dir: str,
    stage: str,
    main_script: str,
    py_files: list,
    stage_args: str,
    aws_region: str,
    executor_memory: str = "4g",
    executor_cores: int = 2,
    total_executor_cores: int = 4,
) -> str:
    """
    spark-submit SSH 명령 문자열 생성.

    Args:
        spark_master_uri:     예) "spark://10.0.1.130:7077"
        job_dir:              Spark Master 작업 경로       예) "/tmp/spark-job"
        stage:                스테이지 디렉토리명           예) "stage1", "stage2"
        main_script:          메인 스크립트 파일명          예) "stage1_anomaly_detection.py"
        py_files:             --py-files 목록 (파일명만)    예) ["connection_stage1.py"]
        stage_args:           스크립트 인자 (Jinja 템플릿 포함 가능)
                              예) "--env stage1 --batch-date {{ ds }}"
        aws_region:           AWS 리전                     예) "ap-northeast-2"
        executor_memory:      executor 메모리               예) "4g"
        executor_cores:       executor 코어 수
        total_executor_cores: 전체 코어 수

    Returns:
        SSHOperator의 command 파라미터에 직접 전달 가능한 문자열.
        stage_args 안의 Jinja 템플릿({{ ds }} 등)은 Airflow가 런타임에 치환한다.
    """
    py_files_str = ",".join(f"{job_dir}/{stage}/{f}" for f in py_files)
    return f"""
        source ~/.bashrc
        /opt/spark/bin/spark-submit \\
            --master {spark_master_uri} \\
            --deploy-mode client \\
            --executor-memory {executor_memory} \\
            --executor-cores {executor_cores} \\
            --total-executor-cores {total_executor_cores} \\
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
            --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider \\
            --conf spark.hadoop.fs.s3a.endpoint=s3.{aws_region}.amazonaws.com \\
            --conf spark.sql.adaptive.enabled=true \\
            --conf spark.sql.adaptive.coalescePartitions.enabled=true \\
            --conf spark.hadoop.fs.s3a.connection.maximum=200 \\
            --conf spark.hadoop.fs.s3a.threads.max=100 \\
            --py-files {py_files_str} \\
            {job_dir}/{stage}/{main_script} \\
            {stage_args}
        """


def build_s3_check_cmd(
    s3_bucket: str,
    prefix: str,
    aws_region: str,
    partition: str = "",
) -> str:
    """
    S3 데이터 존재 확인 bash 명령 생성.

    Args:
        s3_bucket:  S3 버킷명
        prefix:     S3 키 프리픽스     예) "raw-sensor-data", "stage1_anomaly_detected"
        aws_region: AWS 리전
        partition:  파티션 경로 (Jinja 템플릿 포함 가능)
                    예) "dt={{ ds }}"  →  Airflow가 런타임에 날짜로 치환

    Returns:
        BashOperator의 bash_command 파라미터에 직접 전달 가능한 문자열.
    """
    path = "s3://" + s3_bucket + "/" + prefix + ("/" + partition if partition else "") + "/"
    return f"""
        echo "S3 확인: {path}"
        RESULT=$(aws s3 ls "{path}" --region {aws_region} 2>&1)
        if [ -z "$RESULT" ]; then
            echo "ERROR: S3 데이터 없음: {path}"
            exit 1
        fi
        echo "확인 완료"
        echo "$RESULT" | head -5
        """
