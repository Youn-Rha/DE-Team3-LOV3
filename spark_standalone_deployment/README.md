# Spark Standalone 클러스터 배포 패키지

> EC2 3대로 Spark 클러스터를 구축하고 Docker로 처리 코드를 배포하는 완전한 솔루션

## 📁 디렉토리 구조

```
spark_standalone_deployment/
├── README.md                          # 이 파일
├── Dockerfile                         # 처리 애플리케이션 Docker 이미지
├── requirements.txt                   # Python 의존성
│
├── scripts/                           # 배포 및 관리 스크립트
│   ├── setup_spark_node.sh           # EC2 노드 초기 설정
│   ├── build_and_deploy_docker.sh    # Docker 이미지 빌드 및 배포
│   ├── start_cluster.sh               # 클러스터 시작
│   ├── stop_cluster.sh                # 클러스터 종료
│   └── run_stage1_docker.sh           # Stage 1 처리 실행
│
├── spark_configs/                     # Spark 설정 파일 템플릿
│   ├── spark-env.sh.master            # Master 노드용 환경 변수
│   ├── spark-env.sh.worker            # Worker 노드용 환경 변수
│   ├── spark-defaults.conf            # Spark 기본 설정 (모든 노드 공통)
│   └── workers                        # Worker 노드 목록
│
├── spark_standalone_dag.py            # Airflow DAG 파일
│
└── docs/                              # 문서
    ├── AWS_SETUP_GUIDE.md             # AWS 인프라 설정 가이드
    └── DEPLOYMENT_GUIDE.md            # 배포 가이드
```

## 🚀 빠른 시작

### 1단계: AWS 인프라 설정

[docs/AWS_SETUP_GUIDE.md](docs/AWS_SETUP_GUIDE.md) 문서를 따라 진행:

- VPC 및 서브넷 확인
- IAM Role 생성 (S3 접근 권한)
- 보안 그룹 생성
- SSH 키 생성 및 등록
- EC2 인스턴스 3대 생성 (Master 1대, Worker 2대)

### 2단계: Spark 클러스터 배포

[docs/DEPLOYMENT_GUIDE.md](docs/DEPLOYMENT_GUIDE.md) 문서를 따라 진행:

- 각 노드 초기 설정
- Spark 설정 파일 배포
- SSH 키 설정 (Master → Worker)
- 클러스터 시작 및 확인
- Docker 이미지 빌드 및 배포
- 수동 테스트
- Airflow 설정

## 📋 사전 요구사항

### 로컬 환경

- Docker 설치
- AWS CLI 설치 및 설정
- SSH 클라이언트

### AWS 계정

- EC2 인스턴스 생성 권한
- IAM Role 생성 권한
- S3 버킷 접근 권한

## 🏗️ 아키텍처

```
┌─────────────────┐
│  Airflow EC2    │ ← SSH로 Master에 명령 전송
└────────┬────────┘
         │
         ↓
┌─────────────────────────────────────┐
│  Spark Cluster (EC2 3대)            │
│  ┌─────────┐  ┌─────────┐ ┌───────┐│
│  │ Master  │  │Worker 1 │ │Worker2││
│  │t3.medium│  │t3.large │ │t3.large││
│  └─────────┘  └─────────┘ └───────┘│
│  Docker로 처리 코드 실행             │
└────────┬────────────────────────────┘
         │
         ↓
    ┌────────┐
    │   S3   │ ← 데이터 읽기/쓰기
    └────────┘
```

## 🔧 주요 기능

### 1. 자동화된 설정

- `setup_spark_node.sh`: 한 번의 명령으로 노드 초기 설정 완료
- Java, Docker, Spark 자동 설치
- S3 접근용 JAR 파일 자동 다운로드

### 2. Docker 기반 배포

- 환경 일관성 보장
- 코드 변경 시 이미지만 재배포
- 의존성 관리 간편

### 3. 비용 최적화

- 사용하지 않을 때 EC2 자동 중지
- Spot 인스턴스 사용 가능
- 하루 2시간만 운영 시 월 $12.50

### 4. Airflow 통합

- EC2 자동 시작/종료
- Spark 작업 자동 실행
- 실패 시 재시도 로직

## 📊 비용 예상

### 24시간 운영 시

| 항목 | 사양 | 월간 비용 |
|------|------|----------|
| Master | t3.medium | $30.37 |
| Worker 1 | t3.large | $60.74 |
| Worker 2 | t3.large | $60.74 |
| **합계** | | **$151.85** |

### 하루 2시간만 운영 시

| 항목 | 월간 비용 |
|------|----------|
| Master | $2.50 |
| Worker 1 | $5.00 |
| Worker 2 | $5.00 |
| **합계** | **$12.50** |

## 🛠️ 사용 방법

### 클러스터 시작

```bash
cd scripts
bash start_cluster.sh
```

### 처리 작업 실행

```bash
# Master 노드에서
bash run_stage1_docker.sh 2026-02-15
```

### 클러스터 종료

```bash
bash stop_cluster.sh
```

### Docker 이미지 재배포

```bash
# 코드 수정 후
bash build_and_deploy_docker.sh
```

## 📝 설정 파일 수정

배포 전에 다음 파일들의 변수를 실제 값으로 변경해야 합니다:

### 1. scripts/build_and_deploy_docker.sh

```bash
MASTER_IP="<master-public-ip>"
WORKER1_IP="<worker1-public-ip>"
WORKER2_IP="<worker2-public-ip>"
```

### 2. scripts/start_cluster.sh

```bash
MASTER_INSTANCE_ID="i-xxxxxxxxxxxxx"
WORKER1_INSTANCE_ID="i-xxxxxxxxxxxxx"
WORKER2_INSTANCE_ID="i-xxxxxxxxxxxxx"
MASTER_IP="<master-public-ip>"
```

### 3. scripts/stop_cluster.sh

start_cluster.sh와 동일

### 4. spark_configs/spark-env.sh.master

```bash
export SPARK_MASTER_HOST=172.31.10.10  # Master Private IP
```

### 5. spark_configs/spark-env.sh.worker

```bash
export SPARK_MASTER_HOST=172.31.10.10  # Master Private IP
```

### 6. spark_configs/workers

```
172.31.10.11  # Worker 1 Private IP
172.31.10.12  # Worker 2 Private IP
```

### 7. spark_standalone_dag.py

```python
MASTER_INSTANCE_ID = "i-xxxxxxxxxxxxx"
WORKER1_INSTANCE_ID = "i-xxxxxxxxxxxxx"
WORKER2_INSTANCE_ID = "i-xxxxxxxxxxxxx"
MASTER_PRIVATE_IP = "172.31.10.10"
```

## 🔍 모니터링

### Spark Master UI

```
http://<master-public-ip>:8080
```

- Worker 상태 확인
- 실행 중인 애플리케이션 확인

### Spark Application UI

```
http://<master-public-ip>:4040
```

- Jobs, Stages, Executors 상태 확인

### Airflow UI

```
http://<airflow-public-ip>:8080
```

- DAG 실행 상태 확인
- 로그 확인

## ⚠️ 주의사항

### 보안

- SSH 키 파일을 Git에 커밋하지 마세요
- 보안 그룹에서 불필요한 포트는 닫으세요
- IAM Role은 최소 권한 원칙을 따르세요

### 비용

- 사용하지 않을 때는 반드시 EC2를 중지하세요
- CloudWatch 알람을 설정하여 예상치 못한 비용 발생 방지

### 데이터

- S3 Lifecycle 정책으로 오래된 데이터 자동 삭제
- 중요한 데이터는 정기적으로 백업

## 🐛 트러블슈팅

### Spark 클러스터가 시작되지 않음

```bash
# Master에서 로그 확인
tail -f /opt/spark/logs/spark-*.out

# Worker 연결 확인
ssh spark-worker-1 "jps"
```

### S3 접근 오류

```bash
# IAM Role 확인
aws sts get-caller-identity

# S3 접근 테스트
aws s3 ls s3://softeer-7-de3-bucket/
```

### Docker 이미지가 없음

```bash
# 이미지 확인
docker images

# 이미지 재로드
docker load < /tmp/traffic-processing.tar.gz
```

## 📚 참고 문서

- [AWS_SETUP_GUIDE.md](docs/AWS_SETUP_GUIDE.md) - AWS 인프라 설정
- [DEPLOYMENT_GUIDE.md](docs/DEPLOYMENT_GUIDE.md) - 배포 가이드
- [Apache Spark 공식 문서](https://spark.apache.org/docs/latest/)
- [Airflow 공식 문서](https://airflow.apache.org/docs/)

## 🤝 기여

문제가 발생하거나 개선 사항이 있으면 이슈를 등록해주세요.

## 📄 라이선스

이 프로젝트는 MIT 라이선스를 따릅니다.

---

**Made with ❤️ by Softeer DE Team 3**
