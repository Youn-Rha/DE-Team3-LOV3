# Spark Standalone + Airflow 인프라 구성 가이드

## 아키텍처

| EC2 | 역할 | 설명 |
|-----|------|------|
| EC2-1 | Airflow 서버 | DAG 스케줄링, Web UI |
| EC2-2 | Spark Master | spark-submit 실행, 클러스터 관리 |
| EC2-3 | Spark Worker 1 | Executor 실행 |
| EC2-4 | Spark Worker 2 | Executor 실행 |

```
EC2-1 (Airflow) --SSH--> EC2-2 (Master) --SSH--> EC2-3 (Worker1)
                                         --SSH--> EC2-4 (Worker2)
```

## 디렉토리 구조

```
infra/
├── env.sh.template              # 환경변수 템플릿 (IP, 인스턴스 ID 등)
├── spark/
│   ├── scripts/
│   │   ├── setup_spark_node.sh      # Spark 노드 초기 설정
│   │   ├── setup_ssh_keys.sh        # Master → Workers SSH 키 설정
│   │   ├── deploy_spark_configs.sh  # Spark 설정 파일 배포
│   │   ├── start_cluster.sh         # EC2 시작 + Spark 시작
│   │   └── stop_cluster.sh          # Spark 종료 + EC2 중지
│   └── spark_configs/
│       ├── spark-env.sh.master      # Master용 환경변수
│       ├── spark-env.sh.worker      # Worker용 환경변수
│       ├── spark-defaults.conf      # 공통 Spark 설정
│       └── workers                  # Worker 노드 목록
├── airflow/
│   └── setup_airflow.sh             # Airflow 설치 (EC2-1)
└── s3_deploy/
    └── upload_code_to_s3.sh         # 코드 S3 업로드
```

---

## Step 1. AWS 리소스 생성

### 1-1. VPC 및 네트워크 구성

모든 EC2가 같은 VPC/서브넷 안에 있어야 Private IP로 통신 가능하다.

1. **VPC 생성** (이미 있으면 스킵)
   - CIDR: `10.0.0.0/16`
   - DNS 호스트 이름 활성화: `Yes`

2. **퍼블릭 서브넷 생성**
   - CIDR: `10.0.1.0/24`
   - 가용 영역: `ap-northeast-2a`
   - 자동 퍼블릭 IP 할당: `Yes` (Airflow Web UI, Spark Master UI 접속용)

3. **인터넷 게이트웨이 (IGW)**
   - VPC에 연결
   - 라우팅 테이블에 `0.0.0.0/0 → IGW` 추가

### 1-2. 보안 그룹 생성

#### `sg_airflow` (EC2-1: Airflow)

| 유형 | 프로토콜 | 포트 | 소스 | 용도 |
|------|---------|------|------|------|
| Inbound | TCP | 8080 | 내 IP | Airflow Web UI |
| Inbound | TCP | 22 | 내 IP | SSH 접속 |
| Outbound | All | All | 0.0.0.0/0 | 인터넷 |

#### `sg_spark` (EC2-2, EC2-3, EC2-4: Spark 클러스터)

| 유형 | 프로토콜 | 포트 | 소스 | 용도 |
|------|---------|------|------|------|
| Inbound | TCP | 22 | sg_airflow | Airflow → Master SSH |
| Inbound | TCP | 22 | sg_spark | Master ↔ Worker SSH |
| Inbound | TCP | 7077 | sg_spark | Spark Master 포트 |
| Inbound | TCP | 7078 | sg_spark | Spark Worker 포트 |
| Inbound | TCP | 8080 | 내 IP | Spark Master Web UI |
| Inbound | TCP | 8081 | 내 IP | Spark Worker Web UI |
| Inbound | TCP | 4040 | 내 IP | Spark Application UI |
| Outbound | All | All | 0.0.0.0/0 | 인터넷 (S3 접근 등) |

> sg_spark의 Inbound에 자기 자신(sg_spark)을 소스로 추가해야 Master ↔ Worker 간 통신이 된다.

### 1-3. IAM 역할 생성

IAM 콘솔에서 역할을 생성한다. Access Key 대신 Instance Profile을 사용한다.

#### `role-spark-ec2` (EC2-2, EC2-3, EC2-4 에 연결)

Spark 노드가 S3에서 데이터를 읽고 쓸 수 있어야 한다.

1. IAM → 역할 → 역할 만들기
2. 신뢰할 수 있는 엔터티 유형: **AWS 서비스**
3. 사용 사례: **EC2** 선택 → 다음
4. 권한 정책에서 아래 체크:
   - `AmazonS3FullAccess`
5. 역할 이름: `role-spark-ec2` → 역할 생성

#### `role-airflow-ec2` (EC2-1 에 연결)

Airflow에서 EC2 start/stop + S3 데이터 존재 확인이 필요하다.

1. IAM → 역할 → 역할 만들기
2. 신뢰할 수 있는 엔터티 유형: **AWS 서비스**
3. 사용 사례: **EC2** 선택 → 다음
4. 권한 정책에서 아래 체크:
   - `AmazonEC2FullAccess` (EC2 start/stop/describe)
   - `AmazonS3ReadOnlyAccess` (S3 데이터 존재 확인)
5. 역할 이름: `role-airflow-ec2` → 역할 생성

### 1-4. SSH 키 페어 생성

EC2 콘솔 → 키 페어 → 생성:
- 이름: `spark-cluster-key`
- 형식: `.pem`
- 다운로드 후 로컬에 보관: `~/.ssh/spark-cluster-key.pem`

```bash
chmod 400 ~/.ssh/spark-cluster-key.pem
```

### 1-5. EC2 인스턴스 생성

#### EC2-1: Airflow 서버

| 항목 | 값 |
|------|-----|
| AMI | Amazon Linux 2023 |
| 인스턴스 유형 | t3.medium (2 vCPU, 4 GiB) |
| 키 페어 | spark-cluster-key |
| VPC/서브넷 | 위에서 생성한 퍼블릭 서브넷 |
| 보안 그룹 | sg_airflow |
| IAM 역할 | role-airflow-ec2 |
| 스토리지 | 20 GiB gp3 |

#### EC2-2: Spark Master

| 항목 | 값 |
|------|-----|
| AMI | Amazon Linux 2023 |
| 인스턴스 유형 | t3.large (2 vCPU, 8 GiB) |
| 키 페어 | spark-cluster-key |
| VPC/서브넷 | 위에서 생성한 퍼블릭 서브넷 |
| 보안 그룹 | sg_spark |
| IAM 역할 | role-spark-ec2 |
| 스토리지 | 30 GiB gp3 |

#### EC2-3, EC2-4: Spark Workers

| 항목 | 값 |
|------|-----|
| AMI | Amazon Linux 2023 |
| 인스턴스 유형 | t3.large (2 vCPU, 8 GiB) |
| 키 페어 | spark-cluster-key |
| VPC/서브넷 | 위에서 생성한 퍼블릭 서브넷 |
| 보안 그룹 | sg_spark |
| IAM 역할 | role-spark-ec2 |
| 스토리지 | 30 GiB gp3 |

> Worker 수평 확장 시 EC2-4와 동일한 설정으로 추가 생성하면 된다.

인스턴스 생성 후 **Private IP**와 **Instance ID**를 메모한다.

---

## Step 2. 코드 가져오기

각 EC2에 SSH 접속 후 레포지토리를 클론한다.

```bash
# git 설치 (Amazon Linux 2023)
sudo yum install -y git

# 레포 클론 + 브랜치 체크아웃
git clone https://github.com/softeerbootcamp-7th/DE-Team3-LOV3.git
cd DE-Team3-LOV3
git checkout feat/spark-infra-setting
```

> 이미 클론한 경우 최신 코드를 pull 받는다.
> ```bash
> cd DE-Team3-LOV3
> git checkout feat/spark-infra-setting
> git pull origin feat/spark-infra-setting
> ```

---

## Step 3. 환경변수 설정

EC2 생성 후 메모한 값을 `env.sh`에 입력한다.

```bash
cp infra/env.sh.template infra/env.sh
vi infra/env.sh  # 실제 IP, 인스턴스 ID 입력
```

---

## Step 4. Spark 노드 초기 설정 (EC2-2, EC2-3, EC2-4)

각 Spark 노드(Master, Worker1, Worker2)에서 실행:

```bash
bash infra/spark/scripts/setup_spark_node.sh
```

설치 내용: Java 11, Spark 3.5.0, Python3, pip 패키지, S3 JAR

## Step 5. SSH 키 설정 (Master → Workers)

Master 노드에서 실행:

```bash
bash infra/spark/scripts/setup_ssh_keys.sh
```

검증:
```bash
ssh ec2-user@<WORKER1_IP> 'hostname'
ssh ec2-user@<WORKER2_IP> 'hostname'
```

## Step 6. Spark 설정 파일 배포

Master 노드에서 실행 (자기 자신 + Worker에 설정 배포):

```bash
bash infra/spark/scripts/deploy_spark_configs.sh
```

검증:
```bash
cat /opt/spark/conf/spark-env.sh
cat /opt/spark/conf/workers
ssh ec2-user@<WORKER1_IP> 'cat /opt/spark/conf/spark-env.sh'
```

## Step 7. Spark 클러스터 테스트

Master 노드에서 실행:

```bash
# Spark 클러스터 시작
/opt/spark/sbin/start-all.sh
```

확인: `http://<MASTER_PUBLIC_IP>:8080` 에서 Worker 2개 연결 확인

```bash
# Spark 클러스터 종료
/opt/spark/sbin/stop-all.sh
```

> `start_cluster.sh` / `stop_cluster.sh`는 EC2 인스턴스 start/stop을 포함하므로 Airflow(EC2-1)에서 사용한다.
> Master에서 직접 테스트할 때는 `start-all.sh` / `stop-all.sh`만 실행하면 된다.

## Step 8. Airflow 설치 (EC2-1)

```bash
bash infra/airflow/setup_airflow.sh
```

Airflow SSH Connection 추가:
- **Connection Id**: `spark_master`
- **Connection Type**: SSH
- **Host**: `<MASTER_PRIVATE_IP>`
- **Username**: `ec2-user`
- **Extra**: `{"key_file": "<SSH_KEY_PATH>"}`

## Step 9. 코드 S3 업로드

```bash
bash infra/s3_deploy/upload_code_to_s3.sh
```

## Step 10. DAG 실행

1. `airflow_service/dags/pothole_pipeline_dag.py` 를 Airflow DAG 폴더에 배치
2. Airflow Web UI에서 `pothole_pipeline_spark_standalone` DAG 수동 트리거
3. 파이프라인 흐름: S3 입력확인 → 클러스터시작 → 코드다운로드 → Stage1 → S3확인 → Stage2 → S3확인 → 클러스터종료

---

## DAG 파이프라인 흐름

```
check_s3_input → start_cluster → start_spark → download_code
  → run_stage1 → check_s3_stage1_out
  → run_stage2 → check_s3_stage2_out
  → stop_spark → stop_cluster (trigger_rule=all_done)
```

## 비용 절감

- DAG의 `start_cluster`/`stop_cluster`가 EC2를 자동 시작/중지
- 작업이 없을 때는 EC2 인스턴스가 중지 상태
- `stop_cluster`는 `trigger_rule=all_done`으로 실패 시에도 EC2를 중지

---

## Worker 노드 추가 가이드

Worker를 수평 확장하고 싶을 때 아래 순서를 따른다.

### 1. EC2 인스턴스 생성

기존 Worker와 동일한 설정으로 새 EC2를 생성한다.

| 항목 | 값 |
|------|-----|
| AMI | Amazon Linux 2023 |
| 인스턴스 유형 | t3.large (2 vCPU, 8 GiB) |
| 키 페어 | spark-cluster-key |
| VPC/서브넷 | 기존과 동일한 퍼블릭 서브넷 |
| 보안 그룹 | sg_spark |
| IAM 역할 | role-spark-ec2 |
| 스토리지 | 30 GiB gp3 |

생성 후 **Private IP**와 **Instance ID**를 메모한다.

### 2. env.sh 업데이트

`infra/env.sh`에 새 Worker 변수를 추가한다.

```bash
# 기존
export WORKER1_PRIVATE_IP="10.0.1.x"
export WORKER2_PRIVATE_IP="10.0.1.x"
export WORKER1_INSTANCE_ID="i-xxxxx"
export WORKER2_INSTANCE_ID="i-xxxxx"

# 추가
export WORKER3_PRIVATE_IP="<새 Worker Private IP>"
export WORKER3_INSTANCE_ID="<새 Worker Instance ID>"
```

### 3. 새 Worker 노드 초기 설정

새 Worker EC2에 SSH 접속 후 실행:

```bash
bash infra/spark/scripts/setup_spark_node.sh
```

### 4. SSH 키 배포

Master 노드에서 새 Worker로 SSH 키를 배포한다.

```bash
# Master에서 실행
PUB_KEY=$(cat ~/.ssh/id_rsa.pub)
ssh -i <KEY> ec2-user@<새_WORKER_IP> \
  "mkdir -p ~/.ssh && echo '${PUB_KEY}' >> ~/.ssh/authorized_keys && chmod 700 ~/.ssh && chmod 600 ~/.ssh/authorized_keys"
```

검증:
```bash
# Master에서 실행
ssh ec2-user@<새_WORKER_IP> 'hostname'
```

### 5. workers 파일에 새 Worker 추가

`infra/spark/spark_configs/workers` 파일에 새 Worker IP 변수를 추가한다.

```
${WORKER1_PRIVATE_IP}
${WORKER2_PRIVATE_IP}
${WORKER3_PRIVATE_IP}
```

### 6. 설정 파일 재배포

```bash
bash infra/spark/scripts/deploy_spark_configs.sh
```

> `deploy_spark_configs.sh`의 Worker 배포 루프에도 새 IP를 추가해야 한다.
> `for WORKER_IP in "${WORKER1_PRIVATE_IP}" "${WORKER2_PRIVATE_IP}" "${WORKER3_PRIVATE_IP}"`

### 7. Spark 설정 배포 확인

새 Worker에 설정이 잘 들어갔는지 확인:

```bash
ssh -i <KEY> ec2-user@<새_WORKER_IP> 'cat /opt/spark/conf/spark-env.sh'
```

### 8. 클러스터 재시작

Spark 클러스터를 재시작하여 새 Worker를 인식시킨다.

```bash
# Master에서 실행
/opt/spark/sbin/stop-all.sh
/opt/spark/sbin/start-all.sh
```

`http://<MASTER_IP>:8080`에서 Worker 3개가 연결된 것을 확인한다.

### 9. start/stop 스크립트 및 DAG 업데이트

새 Worker의 Instance ID를 EC2 시작/중지 대상에 추가해야 한다.

**`infra/spark/scripts/start_cluster.sh`**, **`stop_cluster.sh`**:
```bash
aws ec2 start-instances \
    --instance-ids ${MASTER_INSTANCE_ID} ${WORKER1_INSTANCE_ID} ${WORKER2_INSTANCE_ID} ${WORKER3_INSTANCE_ID} \
    ...
```

**`airflow_service/dags/pothole_pipeline_dag.py`**:
```python
WORKER3_INSTANCE_ID = "i-xxxxx"  # 추가

# start_cluster, stop_cluster의 --instance-ids에 추가
```

### 요약 체크리스트

- [ ] EC2 생성 (동일 VPC/서브넷/보안그룹/IAM)
- [ ] `env.sh`에 새 Worker IP, Instance ID 추가
- [ ] 새 Worker에서 `setup_spark_node.sh` 실행
- [ ] Master → 새 Worker SSH 키 배포
- [ ] `workers` 파일에 새 IP 추가
- [ ] `deploy_spark_configs.sh` 수정 및 재배포
- [ ] `start_cluster.sh`, `stop_cluster.sh`에 Instance ID 추가
- [ ] DAG에 Instance ID 추가
- [ ] 클러스터 재시작 후 Web UI에서 Worker 수 확인
