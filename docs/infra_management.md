# 인프라 관리 — EC2 클러스터 온디맨드 운영

## 문제

Spark Standalone 클러스터(Master 1대 + Worker 4대)를 상시 운영하면, 파이프라인이 돌지 않는 시간에도 EC2 비용이 계속 발생합니다. 일간 배치 파이프라인은 하루에 한 번, 수십 분만 실행되므로 나머지 23시간은 인스턴스가 유휴 상태입니다.

## 설계: DAG이 인프라 수명주기를 직접 관리

Airflow DAG이 파이프라인 시작 시 EC2를 켜고, 끝나면 끄는 방식으로 실행 시간에만 비용이 발생하도록 했습니다.

```
DAG 시작
  ↓
start_cluster     ← aws ec2 start-instances (5대)
  ↓
start_spark       ← SSH로 Spark Master에서 start-all.sh
  ↓
[파이프라인 실행]
  ↓
stop_spark        ← SSH로 stop-all.sh (trigger_rule=all_done)
  ↓
stop_cluster      ← aws ec2 stop-instances (trigger_rule=all_done)
```

### 클러스터 시작 과정

```bash
# 1. EC2 인스턴스 시작 (Master + Worker 4대)
aws ec2 start-instances --instance-ids $ALL_INSTANCE_IDS

# 2. running 상태까지 대기
aws ec2 wait instance-running --instance-ids $ALL_INSTANCE_IDS

# 3. 네트워크 안정화 대기 (30초)
sleep 30

# 4. Spark Master에서 클러스터 기동
ssh spark-master "/opt/spark/sbin/start-all.sh"
```

`aws ec2 wait instance-running`은 EC2가 running 상태가 될 때까지 폴링합니다. 이후 30초 추가 대기는 SSH 데몬과 네트워크가 완전히 준비될 때까지 기다리는 것입니다. 이 대기 없이 바로 SSH를 시도하면 Connection Refused가 발생할 수 있습니다.

### 실패해도 반드시 종료 (`trigger_rule=all_done`)

파이프라인 중간에 Spark 잡이 실패하면 기본적으로 이후 태스크가 스킵됩니다. 하지만 `stop_spark`과 `stop_cluster`까지 스킵되면 **EC2가 켜진 채로 방치**됩니다.

```python
stop_spark = SSHOperator(
    task_id="stop_spark",
    command="/opt/spark/sbin/stop-all.sh",
    trigger_rule="all_done",   # 성공/실패/스킵 무관하게 실행
)

stop_cluster = BashOperator(
    task_id="stop_cluster",
    bash_command="aws ec2 stop-instances ...",
    trigger_rule="all_done",
)
```

`trigger_rule=all_done`은 **상위 태스크가 어떤 상태로 끝나든 실행**됩니다. 이 설정으로 파이프라인 실패 시에도 EC2 인스턴스가 반드시 종료되어 비용 낭비를 방지합니다.

## 중앙 집중 설정 관리 (`env.sh`)

5대 EC2의 Instance ID, Private IP, AWS 리전, S3 버킷 등 모든 인프라 정보를 `env.sh` 한 파일에서 관리합니다.

```bash
# env.sh (발췌)
MASTER_INSTANCE_ID="i-0abc..."
WORKER1_INSTANCE_ID="i-0def..."
MASTER_PRIVATE_IP="10.0.1.130"
WORKER1_PRIVATE_IP="10.0.1.131"
AWS_REGION="ap-northeast-2"
S3_BUCKET="your-bucket"
```

이 파일을 모든 스크립트(`start_cluster.sh`, `stop_cluster.sh`, `deploy_spark_configs.sh`)와 DAG이 참조합니다. Worker 추가나 IP 변경 시 `env.sh`만 수정하면 됩니다.

## 설정 파일 자동 배포

Spark 설정 파일(`spark-defaults.conf`, `spark-env.sh`, `workers`)은 템플릿으로 관리하고, `deploy_spark_configs.sh`가 `env.sh`의 변수를 치환하여 모든 노드에 배포합니다.

```
spark_configs/ (템플릿)         →    각 노드의 /opt/spark/conf/
├── spark-defaults.conf        →    Master + Worker 4대
├── spark-env.sh.master        →    Master만
├── spark-env.sh.worker        →    Worker 4대
└── workers                    →    Master만 (Worker IP 목록)
```

```bash
# deploy_spark_configs.sh의 핵심 로직
apply_template() {
    sed -e "s|\${MASTER_PRIVATE_IP}|${MASTER_PRIVATE_IP}|g" \
        -e "s|\${WORKER1_PRIVATE_IP}|${WORKER1_PRIVATE_IP}|g" \
        ...
        "$src" > "$dest"
}

# Master: 로컬 복사
apply_template spark-env.sh.master /opt/spark/conf/spark-env.sh

# Workers: SSH로 전송
for WORKER_IP in $WORKER1 $WORKER2 $WORKER3 $WORKER4; do
    scp spark-env.sh ${WORKER_IP}:/opt/spark/conf/
done
```

설정을 직접 편집하지 않고 템플릿에서 생성하므로, 어떤 노드든 동일한 상태를 재현할 수 있습니다.

## Worker 수평 확장

새 Worker를 추가할 때 코드 변경 없이 운영 절차만으로 확장됩니다.

1. 동일 스펙 EC2 생성 (t3.medium, sg_spark, S3FullAccess IAM Role)
2. `env.sh`에 새 Worker의 IP와 Instance ID 추가
3. 초기 설정 스크립트 실행:
   ```bash
   bash setup_spark_node.sh       # Java, Spark 설치
   bash setup_ssh_keys.sh         # Master → Worker SSH 키 배포
   bash deploy_spark_configs.sh   # 설정 파일 배포
   ```
4. DAG의 `start_spark` 태스크가 매 실행마다 `workers` 파일을 갱신하므로 **DAG 코드 수정 불필요**

DAG이 workers 파일을 런타임에 생성하는 이유는, `env.sh`에 Worker를 추가하면 DAG 재배포 없이 다음 실행부터 새 Worker가 자동으로 포함되기 때문입니다.
