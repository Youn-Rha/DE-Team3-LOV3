# AWS 인프라 설정 가이드

> EC2 Standalone Spark 클러스터 구축을 위한 AWS 콘솔 상세 가이드

## 목차

1. [VPC 및 네트워크 설정](#1-vpc-및-네트워크-설정)
2. [IAM Role 생성](#2-iam-role-생성)
3. [보안 그룹 생성](#3-보안-그룹-생성)
4. [SSH 키 생성 및 등록](#4-ssh-키-생성-및-등록)
5. [EC2 인스턴스 생성](#5-ec2-인스턴스-생성)

---

## 1. VPC 및 네트워크 설정

### 1.1 기본 VPC 확인

**AWS 콘솔 → VPC**

1. 왼쪽 메뉴에서 "VPC" 클릭
2. 기본 VPC가 있는지 확인 (Default VPC)
   - 있으면 → 그대로 사용
   - 없으면 → "VPC 생성" 클릭

### 1.2 서브넷 확인

**왼쪽 메뉴 → 서브넷**

1. 가용 영역 `ap-northeast-2a`에 서브넷이 있는지 확인
2. 서브넷 ID 기록 (예: `subnet-0de2d22b0674e3397`)
3. 이 서브넷을 EC2 생성 시 사용

**중요:** 모든 EC2 인스턴스를 동일한 서브넷에 배치하여 Private IP 통신 가능하게 설정

---

## 2. IAM Role 생성

### 2.1 S3 접근용 IAM Role

**AWS 콘솔 → IAM → 역할**

#### Step 1: 역할 만들기

1. 우측 상단 "역할 만들기" 버튼 클릭
2. **신뢰할 수 있는 엔터티 유형**: "AWS 서비스" 선택
3. **사용 사례**: "EC2" 선택
4. "다음" 클릭

#### Step 2: 권한 정책 추가

**방법 1: 기존 정책 사용 (간단)**

1. 검색창에 "S3" 입력
2. `AmazonS3FullAccess` 체크박스 선택
3. "다음" 클릭

**방법 2: 커스텀 정책 생성 (권장 - 보안)**

1. "정책 생성" 버튼 클릭 (새 탭 열림)
2. "JSON" 탭 클릭
3. 다음 JSON 붙여넣기:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3BucketAccess",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::softeer-7-de3-bucket"
    },
    {
      "Sid": "S3ObjectAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::softeer-7-de3-bucket/*"
    }
  ]
}
```

4. "다음: 태그" 클릭
5. "다음: 검토" 클릭
6. 정책 이름: `SparkCluster-S3-Access-Policy`
7. "정책 생성" 클릭
8. 원래 탭으로 돌아가서 새로고침 후 방금 만든 정책 선택

#### Step 3: 역할 이름 지정

1. 역할 이름: `Spark-Cluster-EC2-Role`
2. 설명: "Spark 클러스터 EC2가 S3에 접근하기 위한 역할"
3. "역할 만들기" 클릭

#### Step 4: 역할 ARN 기록

- 생성된 역할 클릭 → ARN 복사
- 예: `arn:aws:iam::123456789012:role/Spark-Cluster-EC2-Role`

### 2.2 Airflow EC2용 IAM Role (선택사항)

Airflow에서 EC2 시작/중지를 위한 권한:

#### 정책 JSON:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:StartInstances",
        "ec2:StopInstances",
        "ec2:DescribeInstances",
        "ec2:DescribeInstanceStatus"
      ],
      "Resource": "*"
    }
  ]
}
```

#### 역할 이름:
- `Airflow-EC2-Management-Role`

---

## 3. 보안 그룹 생성

### 3.1 Spark 클러스터용 보안 그룹

**AWS 콘솔 → EC2 → 네트워크 및 보안 → 보안 그룹**

#### Step 1: 보안 그룹 생성

1. "보안 그룹 생성" 버튼 클릭
2. **보안 그룹 이름**: `spark-cluster-sg`
3. **설명**: "Spark 클러스터 보안 그룹"
4. **VPC**: 기본 VPC 선택

#### Step 2: 인바운드 규칙 추가

"인바운드 규칙" 섹션에서 "규칙 추가" 버튼으로 다음 규칙 추가:

| 유형 | 프로토콜 | 포트 범위 | 소스 | 설명 |
|------|---------|----------|------|------|
| SSH | TCP | 22 | 내 IP | SSH 접속 (개발용) |
| SSH | TCP | 22 | Airflow EC2 보안 그룹 | Airflow에서 SSH 접속 |
| 사용자 지정 TCP | TCP | 7077 | spark-cluster-sg | Spark Master 포트 |
| 사용자 지정 TCP | TCP | 7078 | spark-cluster-sg | Spark Worker 포트 |
| 사용자 지정 TCP | TCP | 8080 | 내 IP | Spark Master UI |
| 사용자 지정 TCP | TCP | 8081 | 내 IP | Spark Worker UI |
| 사용자 지정 TCP | TCP | 4040 | 내 IP | Spark Application UI |
| 모든 트래픽 | 모두 | 모두 | spark-cluster-sg | 클러스터 내부 통신 |

**중요 설정:**

1. **"내 IP"** 선택 방법:
   - 소스 입력창 클릭 → 드롭다운에서 "내 IP" 선택
   - 자동으로 현재 IP 주소 입력됨

2. **"spark-cluster-sg"** 선택 방법:
   - 소스 입력창 클릭 → 드롭다운에서 방금 만든 보안 그룹 선택
   - 이렇게 하면 같은 보안 그룹 내 인스턴스끼리 통신 가능

#### Step 3: 아웃바운드 규칙

- 기본값 유지 (모든 트래픽 허용)

#### Step 4: 보안 그룹 생성

- "보안 그룹 생성" 버튼 클릭
- 보안 그룹 ID 기록 (예: `sg-0d58986750dfa5729`)

### 3.2 Airflow용 보안 그룹 (선택사항)

별도 Airflow EC2를 사용하는 경우:

| 유형 | 프로토콜 | 포트 범위 | 소스 | 설명 |
|------|---------|----------|------|------|
| SSH | TCP | 22 | 내 IP | SSH 접속 |
| 사용자 지정 TCP | TCP | 8080 | 내 IP | Airflow UI |
| 모든 트래픽 | 모두 | 모두 | 0.0.0.0/0 | 아웃바운드 (AWS CLI 사용) |

---

## 4. SSH 키 생성 및 등록

### 4.1 로컬에서 SSH 키 생성

**Mac/Linux 터미널:**

```bash
# SSH 키 생성 (비밀번호 없이)
ssh-keygen -t rsa -b 4096 -f ~/.ssh/spark-cluster-key -N ""

# 생성 확인
ls -la ~/.ssh/spark-cluster-key*
# spark-cluster-key (프라이빗 키)
# spark-cluster-key.pub (퍼블릭 키)

# 권한 설정
chmod 400 ~/.ssh/spark-cluster-key
```

**Windows PowerShell:**

```powershell
# SSH 키 생성
ssh-keygen -t rsa -b 4096 -f $env:USERPROFILE\.ssh\spark-cluster-key -N ""

# 생성 확인
dir $env:USERPROFILE\.ssh\spark-cluster-key*
```

### 4.2 공개 키 내용 확인

**Mac/Linux:**

```bash
cat ~/.ssh/spark-cluster-key.pub
```

**Windows:**

```powershell
Get-Content $env:USERPROFILE\.ssh\spark-cluster-key.pub
```

**출력 예시:**
```
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDXxxx... user@hostname
```

이 전체 내용을 복사 (Ctrl+C / Cmd+C)

### 4.3 AWS에 공개 키 등록

**AWS 콘솔 → EC2 → 네트워크 및 보안 → 키 페어**

1. 우측 상단 "작업" 드롭다운 클릭
2. "키 페어 가져오기" 선택
3. **이름**: `spark-cluster-key`
4. **키 페어 파일 내용**: 위에서 복사한 공개 키 전체 붙여넣기
5. "키 페어 가져오기" 버튼 클릭

**확인:**
- 키 페어 목록에 `spark-cluster-key` 표시되어야 함

---

## 5. EC2 인스턴스 생성

### 5.1 Master 노드 생성

**AWS 콘솔 → EC2 → 인스턴스**

#### Step 1: 인스턴스 시작

1. "인스턴스 시작" 버튼 클릭

#### Step 2: 이름 및 태그

- **이름**: `spark-master`
- **태그 추가** (선택사항):
  - Key: `Role`, Value: `Master`
  - Key: `Project`, Value: `Pothole-Detection`
  - Key: `Environment`, Value: `Production`

#### Step 3: 애플리케이션 및 OS 이미지 (AMI)

- **Amazon Machine Image**: "Amazon Linux 2023 AMI" 선택
- **아키텍처**: 64비트 (x86)

#### Step 4: 인스턴스 유형

- **인스턴스 유형**: `t3.medium` 선택
  - vCPU: 2
  - 메모리: 4 GiB
  - 시간당 요금: 약 $0.0416

#### Step 5: 키 페어

- **키 페어 이름**: `spark-cluster-key` 선택

#### Step 6: 네트워크 설정

- **VPC**: 기본 VPC 선택
- **서브넷**: `ap-northeast-2a` 서브넷 선택
- **퍼블릭 IP 자동 할당**: "활성화"
- **방화벽(보안 그룹)**: "기존 보안 그룹 선택"
  - `spark-cluster-sg` 체크박스 선택

#### Step 7: 스토리지 구성

- **루트 볼륨**:
  - 크기: `30 GiB` (기본 8GB에서 증가)
  - 볼륨 유형: `gp3` (범용 SSD)
  - IOPS: 3000 (기본값)
  - 처리량: 125 MB/s (기본값)

#### Step 8: 고급 세부 정보

- **IAM 인스턴스 프로파일**: `Spark-Cluster-EC2-Role` 선택
- **종료 방지**: 비활성화 (기본값)
- **종료 동작**: 중지 (기본값)
- **사용자 데이터**: 비워둠

#### Step 9: 요약 및 시작

1. 우측 "요약" 섹션에서 설정 확인
2. **인스턴스 수**: 1
3. "인스턴스 시작" 버튼 클릭

#### Step 10: 인스턴스 정보 기록

인스턴스 생성 후:

1. 인스턴스 목록에서 `spark-master` 클릭
2. 하단 "세부 정보" 탭에서 다음 정보 기록:

```
Master 노드:
- Instance ID: i-0123456789abcdef0
- Public IPv4 address: 3.35.123.45
- Private IPv4 address: 172.31.10.10
- Availability Zone: ap-northeast-2a
```

### 5.2 Worker 노드 1 생성

**Master 노드와 동일한 과정 반복, 다음만 변경:**

#### 변경 사항:

- **이름**: `spark-worker-1`
- **인스턴스 유형**: `t3.large` (vCPU: 2, 메모리: 8 GiB)
- **태그**: Key: `Role`, Value: `Worker`
- **나머지**: Master와 동일

#### 정보 기록:

```
Worker 1:
- Instance ID: i-0123456789abcdef1
- Public IPv4 address: 3.35.123.46
- Private IPv4 address: 172.31.10.11
```

### 5.3 Worker 노드 2 생성

**Worker 1과 동일한 과정 반복:**

- **이름**: `spark-worker-2`
- **나머지**: Worker 1과 동일

#### 정보 기록:

```
Worker 2:
- Instance ID: i-0123456789abcdef2
- Public IPv4 address: 3.35.123.47
- Private IPv4 address: 172.31.10.12
```

### 5.4 Airflow EC2 생성 (선택사항)

별도 Airflow 서버를 사용하는 경우:

- **이름**: `airflow-server`
- **인스턴스 유형**: `t3.small` (vCPU: 2, 메모리: 2 GiB)
- **키 페어**: `spark-cluster-key` (동일한 키 사용)
- **보안 그룹**: `airflow-sg` (위에서 생성한 것)
- **IAM 역할**: `Airflow-EC2-Management-Role`

---

## 6. 설정 정보 정리

### 6.1 모든 정보를 한 곳에 정리

다음 템플릿을 복사하여 실제 값으로 채우세요:

```yaml
# AWS 인프라 정보

vpc:
  vpc_id: vpc-xxxxxxxxxxxxx
  subnet_id: subnet-0de2d22b0674e3397
  region: ap-northeast-2
  availability_zone: ap-northeast-2a

security_groups:
  spark_cluster_sg: sg-0d58986750dfa5729
  airflow_sg: sg-xxxxxxxxxxxxx

iam_roles:
  spark_cluster_role: Spark-Cluster-EC2-Role
  spark_cluster_role_arn: arn:aws:iam::123456789012:role/Spark-Cluster-EC2-Role
  airflow_role: Airflow-EC2-Management-Role

ssh_key:
  key_name: spark-cluster-key
  private_key_path: ~/.ssh/spark-cluster-key
  public_key_path: ~/.ssh/spark-cluster-key.pub

ec2_instances:
  master:
    instance_id: i-0123456789abcdef0
    public_ip: 3.35.123.45
    private_ip: 172.31.10.10
    instance_type: t3.medium
    
  worker_1:
    instance_id: i-0123456789abcdef1
    public_ip: 3.35.123.46
    private_ip: 172.31.10.11
    instance_type: t3.large
    
  worker_2:
    instance_id: i-0123456789abcdef2
    public_ip: 3.35.123.47
    private_ip: 172.31.10.12
    instance_type: t3.large
    
  airflow:
    instance_id: i-0123456789abcdef3
    public_ip: 3.35.123.48
    private_ip: 172.31.10.13
    instance_type: t3.small

s3:
  bucket_name: softeer-7-de3-bucket
  region: ap-northeast-2
```

### 6.2 스크립트 파일 업데이트

위 정보를 바탕으로 다음 파일들의 변수를 실제 값으로 변경:

1. `scripts/build_and_deploy_docker.sh`
2. `scripts/start_cluster.sh`
3. `scripts/stop_cluster.sh`
4. `spark_configs/spark-env.sh.master`
5. `spark_configs/spark-env.sh.worker`
6. `spark_configs/workers`
7. `spark_standalone_dag.py`

---

## 7. 비용 예상

### 월별 예상 비용 (서울 리전 기준)

#### 24시간 운영 시:

| 항목 | 사양 | 시간당 | 월간 (730시간) |
|------|------|--------|---------------|
| Master | t3.medium | $0.0416 | $30.37 |
| Worker 1 | t3.large | $0.0832 | $60.74 |
| Worker 2 | t3.large | $0.0832 | $60.74 |
| **합계** | | | **$151.85** |

#### 하루 2시간만 운영 시:

| 항목 | 월간 (60시간) |
|------|--------------|
| Master | $2.50 |
| Worker 1 | $5.00 |
| Worker 2 | $5.00 |
| **합계** | **$12.50** |

**추가 비용:**
- S3 스토리지: 데이터량에 따라
- S3 요청: 읽기/쓰기 횟수에 따라
- 데이터 전송: 아웃바운드 트래픽에 따라

**비용 절감 팁:**
- 사용하지 않을 때 EC2 중지
- Spot 인스턴스 사용 (최대 70% 절감)
- S3 Lifecycle 정책으로 오래된 데이터 삭제

---

## 8. 보안 체크리스트

배포 전 반드시 확인:

- [ ] IAM Role이 최소 권한 원칙 준수
- [ ] 보안 그룹에서 불필요한 포트 차단
- [ ] SSH 접근을 특정 IP로 제한
- [ ] 프라이빗 키 파일 권한 400으로 설정
- [ ] 프라이빗 키를 Git에 커밋하지 않음 (.gitignore 확인)
- [ ] EC2 인스턴스에 퍼블릭 IP 필요 시에만 할당
- [ ] S3 버킷 암호화 활성화
- [ ] CloudWatch 로그 활성화 (선택사항)

---

## 9. 다음 단계

AWS 인프라 설정 완료 후:

1. [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) - Spark 클러스터 배포
2. [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - 문제 해결 가이드

---

## 참고 자료

- [AWS EC2 사용 설명서](https://docs.aws.amazon.com/ec2/)
- [AWS IAM 모범 사례](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
- [AWS VPC 가이드](https://docs.aws.amazon.com/vpc/)
- [Apache Spark 공식 문서](https://spark.apache.org/docs/latest/)
