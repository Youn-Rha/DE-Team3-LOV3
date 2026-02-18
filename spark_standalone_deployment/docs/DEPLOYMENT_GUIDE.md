# Spark Standalone 클러스터 배포 가이드

> EC2 3대로 Spark 클러스터를 구축하고 Docker로 처리 코드를 배포하는 전체 과정

## 전제 조건

- [AWS_SETUP_GUIDE.md](AWS_SETUP_GUIDE.md) 완료
- EC2 인스턴스 3대 생성 완료
- SSH 키 설정 완료
- 로컬에 Docker 설치

---

## 목차

1. [각 노드 초기 설정](#1-각-노드-초기-설정)
2. [Spark 설정 파일 배포](#2-spark-설정-파일-배포)
3. [SSH 키 설정 (Master → Worker)](#3-ssh-키-설정-master--worker)
4. [Spark 클러스터 시작 및 확인](#4-spark-클러스터-시작-및-확인)
5. [Docker 이미지 빌드 및 배포](#5-docker-이미지-빌드-및-배포)
6. [수동 테스트](#6-수동-테스트)
7. [Airflow 설정](#7-airflow-설정)

---

## 1. 각 노드 초기 설정

### 1.1 Master 노드 설정

#### Step 1: SSH 접속

```bash
ssh -i ~/.ssh/spark-cluster-key ec2-user@<master-public-ip>
```

#### Step 2: 설정 스크립트 전송 및 실행

**로컬 터미널에서:**

```bash
# 스크립트 전송
scp -i ~/.ssh/spark-cluster-key \
    spark_standalone_deployment/scripts/setup_spark_node.sh \
    ec2-user@<master-public-ip>:/tmp/
```

**Master 노드에서:**

```bash
# 스크립트 실행
bash /tmp/setup_spark_node.sh

# 완료 후 재로그인 (Docker 그룹 적용)
exit
ssh -i ~/.ssh/spark-cluster-key ec2-user@<master-public-ip>

# 설치 확인
java -version
docker --version
/opt/spark/bin/spark-shell --version
```

### 1.2 Worker 노드 1 설정

**로컬 터미널에서:**

```bash
# 스크립트 전송
scp -i ~/.ssh/spark-cluster-key \
    spark_standalone_deployment/scripts/setup_spark_node.sh \
    ec2-user@<worker1-public-ip>:/tmp/

# SSH 접속
ssh -i ~/.ssh/spark-cluster-key ec2-user@<worker1-public-ip>

# 스크립트 실행
bash /tmp/setup_spark_node.sh

# 재로그인
exit
ssh -i ~/.ssh/spark-cluster-key ec2-user@<worker1-public-ip>
```

### 1.3 Worker 노드 2 설정

Worker 1과 동일한 과정 반복 (Public IP만 Worker 2로 변경)

---

## 2. Spark 설정 파일 배포

### 2.1 Master 노드 설정 파일

#### Step 1: spark-env.sh 생성

**Master 노드에서:**

```bash
sudo vi /opt/spark/conf/spark-env.sh
```

**내용 입력 (i 키로 입력 모드):**

```bash
#!/usr/bin/env bash

# Master 설정 (실제 Master Private IP로 변경)
export SPARK_MASTER_HOST=172.31.10.10
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

# Worker 리소스 설정
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=3g
export SPARK_WORKER_PORT=7078
export SPARK_WORKER_WEBUI_PORT=8081

# Java 경로
export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto

# Python 경로
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# 로그 설정
export SPARK_LOG_DIR=/opt/spark/logs
export SPARK_PID_DIR=/opt/spark/run
```

**저장 및 종료:** `ESC` → `:wq` → `Enter`

#### Step 2: workers 파일 생성

```bash
sudo vi /opt/spark/conf/workers
```

**내용 (실제 Worker Private IP로 변경):**

```
172.31.10.11
172.31.10.12
```

**저장:** `ESC` → `:wq` → `Enter`

#### Step 3: spark-defaults.conf 생성

```bash
sudo vi /opt/spark/conf/spark-defaults.conf
```

**내용:**

```properties
# S3 접근 설정
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider
spark.hadoop.fs.s3a.endpoint=s3.ap-northeast-2.amazonaws.com
spark.hadoop.fs.s3a.path.style.access=false

# S3 성능 최적화
spark.hadoop.fs.s3a.connection.ssl.enabled=true
spark.hadoop.fs.s3a.fast.upload=true
spark.hadoop.fs.s3a.fast.upload.buffer=disk
spark.hadoop.fs.s3a.multipart.size=104857600
spark.hadoop.fs.s3a.connection.maximum=100
spark.hadoop.fs.s3a.threads.max=50

# Spark 기본 설정
spark.executor.memory=4g
spark.executor.cores=2
spark.driver.memory=2g

# 로그 설정
spark.eventLog.enabled=true
spark.eventLog.dir=/opt/spark/spark-events
spark.history.fs.logDirectory=/opt/spark/spark-events

# 네트워크 타임아웃
spark.network.timeout=800s
spark.executor.heartbeatInterval=60s
```

**저장:** `ESC` → `:wq` → `Enter`

#### Step 4: /etc/hosts 파일 수정

```bash
sudo vi /etc/hosts
```

**파일 끝에 추가 (실제 IP로 변경):**

```
172.31.10.10  spark-master
172.31.10.11  spark-worker-1
172.31.10.12  spark-worker-2
```

**저장:** `ESC` → `:wq` → `Enter`

#### Step 5: 로그 디렉토리 생성

```bash
mkdir -p /opt/spark/logs
mkdir -p /opt/spark/run
mkdir -p /opt/spark/spark-events
```

### 2.2 Worker 노드 설정 파일

**Worker 1과 Worker 2에서 각각 실행:**

#### Step 1: spark-env.sh

```bash
sudo vi /opt/spark/conf/spark-env.sh
```

**내용:**

```bash
#!/usr/bin/env bash

# Master 정보 (실제 Master Private IP로 변경)
export SPARK_MASTER_HOST=172.31.10.10
export SPARK_MASTER_PORT=7077

# Worker 리소스 설정
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=6g
export SPARK_WORKER_PORT=7078
export SPARK_WORKER_WEBUI_PORT=8081

# Java 경로
export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto

# Python 경로
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# 로그 설정
export SPARK_LOG_DIR=/opt/spark/logs
export SPARK_PID_DIR=/opt/spark/run
```

#### Step 2: spark-defaults.conf

Master와 동일한 내용 복사

#### Step 3: /etc/hosts

Master와 동일한 내용 추가

#### Step 4: 로그 디렉토리

```bash
mkdir -p /opt/spark/logs
mkdir -p /opt/spark/run
mkdir -p /opt/spark/spark-events
```

---

## 3. SSH 키 설정 (Master → Worker)

Master에서 Worker로 비밀번호 없이 SSH 접속 가능하도록 설정

### 3.1 Master 노드에서 SSH 키 생성

```bash
# Master 노드에서
ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa

# 공개 키 확인
cat ~/.ssh/id_rsa.pub
```

### 3.2 Worker 노드에 공개 키 복사

**방법 1: ssh-copy-id 사용**

```bash
# Master 노드에서
ssh-copy-id -i ~/.ssh/id_rsa.pub ec2-user@spark-worker-1
ssh-copy-id -i ~/.ssh/id_rsa.pub ec2-user@spark-worker-2
```

**방법 2: 수동 복사**

```bash
# Master에서 공개 키 복사
cat ~/.ssh/id_rsa.pub

# Worker 1에 접속
ssh ec2-user@spark-worker-1

# Worker 1에서
mkdir -p ~/.ssh
vi ~/.ssh/authorized_keys
# Master의 공개 키 붙여넣기
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
exit

# Worker 2도 동일하게 반복
```

### 3.3 테스트

```bash
# Master 노드에서
ssh spark-worker-1
# 비밀번호 없이 접속되어야 함
exit

ssh spark-worker-2
exit
```

---

## 4. Spark 클러스터 시작 및 확인

### 4.1 클러스터 시작

**Master 노드에서:**

```bash
/opt/spark/sbin/start-all.sh
```

**출력 예시:**
```
starting org.apache.spark.deploy.master.Master, logging to /opt/spark/logs/spark-ec2-user-org.apache.spark.deploy.master.Master-1-spark-master.out
spark-worker-1: starting org.apache.spark.deploy.worker.Worker, logging to /opt/spark/logs/spark-ec2-user-org.apache.spark.deploy.worker.Worker-1-spark-worker-1.out
spark-worker-2: starting org.apache.spark.deploy.worker.Worker, logging to /opt/spark/logs/spark-ec2-user-org.apache.spark.deploy.worker.Worker-1-spark-worker-2.out
```

### 4.2 클러스터 상태 확인

#### 방법 1: 웹 UI (권장)

브라우저에서 접속:
```
http://<master-public-ip>:8080
```

**확인 사항:**
- "Workers" 섹션에 2개 Worker가 "ALIVE" 상태
- 각 Worker의 메모리와 코어 수 확인

#### 방법 2: 명령어

```bash
# Master에서
jps
# 출력에 "Master" 있어야 함

# Worker 확인
ssh spark-worker-1 "jps"
# 출력에 "Worker" 있어야 함

ssh spark-worker-2 "jps"
```

### 4.3 S3 접근 테스트

```bash
# Master 노드에서
aws s3 ls s3://softeer-7-de3-bucket/

# 버킷 내용이 출력되면 IAM Role이 제대로 설정된 것
```

---

## 5. Docker 이미지 빌드 및 배포

### 5.1 로컬에서 Docker 이미지 빌드

**로컬 터미널에서 (프로젝트 루트):**

```bash
# 이미지 빌드
docker build -t traffic-processing:latest -f spark_standalone_deployment/Dockerfile .

# 빌드 확인
docker images | grep traffic-processing
```

### 5.2 이미지를 tar.gz로 저장

```bash
docker save traffic-processing:latest | gzip > /tmp/traffic-processing.tar.gz

# 파일 크기 확인
du -h /tmp/traffic-processing.tar.gz
```

### 5.3 각 노드에 전송

```bash
# Master에 전송
scp -i ~/.ssh/spark-cluster-key \
    /tmp/traffic-processing.tar.gz \
    ec2-user@<master-public-ip>:/tmp/

# Worker 1에 전송
scp -i ~/.ssh/spark-cluster-key \
    /tmp/traffic-processing.tar.gz \
    ec2-user@<worker1-public-ip>:/tmp/

# Worker 2에 전송
scp -i ~/.ssh/spark-cluster-key \
    /tmp/traffic-processing.tar.gz \
    ec2-user@<worker2-public-ip>:/tmp/
```

### 5.4 각 노드에서 이미지 로드

**Master 노드:**

```bash
ssh -i ~/.ssh/spark-cluster-key ec2-user@<master-public-ip>

# 이미지 로드
docker load < /tmp/traffic-processing.tar.gz

# 확인
docker images

# 임시 파일 삭제
rm /tmp/traffic-processing.tar.gz

exit
```

**Worker 1, 2도 동일하게 반복**

---

## 6. 수동 테스트

### 6.1 Master에서 직접 실행

**Master 노드에서:**

```bash
docker run --rm \
  --network host \
  -e AWS_DEFAULT_REGION=ap-northeast-2 \
  -e BATCH_DATE=2026-02-15 \
  traffic-processing:latest \
  spark-submit \
    --master spark://172.31.10.10:7077 \
    --deploy-mode client \
    --executor-memory 4g \
    --executor-cores 2 \
    --total-executor-cores 4 \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider \
    --conf spark.hadoop.fs.s3a.endpoint=s3.ap-northeast-2.amazonaws.com \
    /app/processing_service/stage1/stage1_anomaly_detection.py \
    --env stage1 \
    --batch-date 2026-02-15
```

### 6.2 실행 확인

#### Spark UI에서 확인:

```
http://<master-public-ip>:4040
```

**확인 사항:**
- Jobs 탭: 실행 중인 작업
- Stages 탭: 각 Stage 진행 상황
- Executors 탭: Worker 2개가 활성화되어 있는지

#### S3 결과 확인:

```bash
aws s3 ls s3://softeer-7-de3-bucket/stage1_anomaly_detected/dt=2026-02-15/
```

---

## 7. Airflow 설정

### 7.1 Airflow EC2에 SSH 키 복사

**로컬에서:**

```bash
# Airflow EC2로 키 복사
scp -i ~/.ssh/spark-cluster-key \
    ~/.ssh/spark-cluster-key \
    ec2-user@<airflow-public-ip>:~/.ssh/

# Airflow EC2 접속
ssh -i ~/.ssh/spark-cluster-key ec2-user@<airflow-public-ip>

# 권한 설정
chmod 400 ~/.ssh/spark-cluster-key

# Master 접속 테스트
ssh -i ~/.ssh/spark-cluster-key ec2-user@172.31.10.10 "echo 'Connection OK'"
```

### 7.2 Airflow에서 SSH Connection 추가

#### Airflow UI 접속:

```
http://<airflow-public-ip>:8080
```

#### Connection 추가:

1. 상단 메뉴 "Admin" → "Connections" 클릭
2. 우측 상단 "+" 버튼 클릭
3. 다음 정보 입력:

```
Connection Id: spark_master
Connection Type: SSH
Host: 172.31.10.10  (Master Private IP)
Username: ec2-user
Port: 22
Extra: {"key_file": "/home/ec2-user/.ssh/spark-cluster-key"}
```

4. "Save" 버튼 클릭

### 7.3 DAG 파일 배포

**로컬에서:**

```bash
# DAG 파일 전송
scp -i ~/.ssh/spark-cluster-key \
    spark_standalone_deployment/spark_standalone_dag.py \
    ec2-user@<airflow-public-ip>:~/airflow/dags/
```

### 7.4 DAG 활성화

1. Airflow UI에서 DAG 목록 확인 (1-2분 소요)
2. `spark_standalone_traffic_processing` DAG 찾기
3. 토글 버튼을 ON으로 변경

### 7.5 수동 실행 테스트

1. DAG 이름 클릭
2. 우측 상단 "Trigger DAG" 버튼 클릭
3. "Graph" 탭에서 실행 상태 확인
4. 각 Task 클릭 → "Log" 버튼으로 로그 확인

---

## 8. 클러스터 관리

### 8.1 클러스터 시작

**로컬에서:**

```bash
cd spark_standalone_deployment/scripts

# 스크립트 변수 수정 후
bash start_cluster.sh
```

### 8.2 클러스터 종료

```bash
bash stop_cluster.sh
```

### 8.3 로그 확인

```bash
# Master 로그
ssh -i ~/.ssh/spark-cluster-key ec2-user@<master-public-ip>
tail -f /opt/spark/logs/spark-*.out

# Docker 컨테이너 로그
docker ps  # 실행 중인 컨테이너 확인
docker logs <container-id>
```

---

## 9. 트러블슈팅 체크리스트

문제 발생 시 다음 항목 확인:

### 9.1 EC2 관련

- [ ] IAM Role이 EC2에 연결되어 있는지
- [ ] 보안 그룹 포트가 올바르게 열려 있는지
- [ ] 모든 인스턴스가 같은 서브넷에 있는지
- [ ] Private IP가 올바르게 설정되어 있는지

### 9.2 Spark 관련

- [ ] Spark 클러스터가 시작되었는지 (jps 명령어)
- [ ] Worker가 Master에 연결되었는지 (Web UI 확인)
- [ ] /etc/hosts 파일이 올바르게 설정되어 있는지
- [ ] spark-env.sh의 IP 주소가 정확한지

### 9.3 S3 관련

- [ ] IAM Role에 S3 권한이 있는지
- [ ] S3 경로가 `s3a://`로 시작하는지
- [ ] spark-defaults.conf에 S3 설정이 있는지
- [ ] hadoop-aws JAR 파일이 있는지

### 9.4 Docker 관련

- [ ] Docker 이미지가 모든 노드에 로드되어 있는지
- [ ] Docker가 실행 중인지 (docker ps)
- [ ] --network host 옵션이 있는지

### 9.5 Airflow 관련

- [ ] SSH Connection이 올바르게 설정되어 있는지
- [ ] Airflow EC2에 SSH 키가 복사되어 있는지
- [ ] DAG 파일이 dags 폴더에 있는지
- [ ] AWS CLI 권한이 있는지 (EC2 시작/중지용)

---

## 10. 다음 단계

배포 완료 후:

1. 모니터링 설정 (CloudWatch, Spark History Server)
2. 자동 백업 설정 (S3 Lifecycle)
3. 알람 설정 (작업 실패 시 알림)
4. 성능 튜닝 (메모리, 코어 수 조정)

---

## 참고 자료

- [Apache Spark Standalone Mode](https://spark.apache.org/docs/latest/spark-standalone.html)
- [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Airflow SSH Operator](https://airflow.apache.org/docs/apache-airflow-providers-ssh/stable/operators.html)
