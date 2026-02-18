# EC2 배포 가이드 (Amazon Linux 2023)

## 인스턴스 요구사항

- **AMI**: Amazon Linux 2023
- **인스턴스 타입**: t3.small 이상 (2 vCPU, 2 GiB RAM)
- **디스크**: 20 GiB gp3
- **IAM Role**: S3 읽기 권한 (`AmazonS3ReadOnlyAccess`)
- **보안 그룹**: 22 (SSH), 5432 (PostgreSQL - 필요 시)

## 1. Docker 설치

```bash
# 패키지 업데이트
sudo dnf update -y

# Docker 설치 및 시작
sudo dnf install -y docker
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user

# docker-compose 설치
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# 그룹 변경 적용 (재접속 또는)
newgrp docker
```

## 2. 코드 클론

```bash
sudo dnf install -y git
cd /home/ec2-user
git clone -b fix/serving_service https://github.com/softeerbootcamp-7th/DE-Team3-LOV3.git
cd DE-Team3-LOV3/serving_service
```

## 3. 환경 변수 설정

```bash
cat > .env << 'EOF'
POSTGRES_PASSWORD=your-strong-postgres-password
POSTGRES_HOST=localhost
S3_BUCKET=pothole-detection-results
EOF
```

> `.env`의 비밀번호를 반드시 강력한 값으로 변경하세요.

## 4. PostgreSQL 기동

```bash
# 데이터 디렉토리 생성
sudo mkdir -p /data/postgres
sudo chmod 755 /data/postgres

# PostgreSQL 컨테이너 시작
docker-compose up -d

# 상태 확인
docker-compose ps
docker-compose logs postgres
```

## 5. Python 환경 설정

```bash
sudo dnf install -y python3 python3-pip
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 6. PotholeLoader 실행 (S3 -> PostgreSQL)

```bash
source venv/bin/activate

python -m loaders.pothole_loader \
  --s3-path "s3://pothole-detection-results/stage2/parquet/dt=2026-02-12/" \
  --date "2026-02-12" \
  --config config.yaml
```

## 7. ComplaintLoader 실행 (공공데이터 API -> PostgreSQL)

863호선 범위 내 포트홀 민원 데이터를 1회 적재합니다.

```bash
source venv/bin/activate

python -m loaders.complaint_loader \
  --service-key "공공데이터_API_인증키" \
  --date-from 20260201 --date-to 20260215 \
  --road-network ../ingestion_service/producer/road_network_863.csv \
  --config config.yaml
```

## 8. 데이터 확인

```bash
# PostgreSQL 접속
docker-compose exec postgres psql -U pothole_user -d road_safety

# 테이블 확인
\dt

# 뷰 확인
\dv

# 민원 핫스팟 조회
SELECT * FROM complaint_hotspot LIMIT 5;

# 민원 건수 확인
SELECT COUNT(*) FROM pothole_complaints;

# 종료
\q
```

## 기본 운영 명령어

```bash
# 상태 확인
docker-compose ps

# 로그 보기
docker-compose logs -f postgres

# PostgreSQL 재시작
docker-compose restart postgres

# 서비스 중지/시작
docker-compose down
docker-compose up -d
```

## 트러블슈팅

**PostgreSQL 연결 실패**
```bash
docker-compose logs postgres
docker-compose restart postgres
# .env의 POSTGRES_PASSWORD 확인
```

**디스크 부족**
```bash
df -h /data
docker system prune
```

**API 호출 실패 (ComplaintLoader)**
```bash
# 인증키 확인 (URL 인코딩 주의)
curl "https://apis.data.go.kr/B553881/Pothole/minPotholeLatLonInfo?serviceKey=YOUR_KEY&dateFrom=20260201&dateTo=20260215&type=json&numOfRows=1"
```

**Docker 권한 오류**
```bash
sudo usermod -aG docker ec2-user
newgrp docker
```

## IAM Role 설정 (S3 접근)

EC2 인스턴스가 S3에 접근하려면 IAM Role이 필요합니다.

1. **IAM** -> **Roles** -> **Create role**
2. **Trusted entity type**: AWS service -> EC2
3. **Permissions**: `AmazonS3ReadOnlyAccess` 추가
4. **Role name**: `pothole-s3-reader`
5. EC2 인스턴스에 Role 연결:
   - EC2 콘솔 -> 인스턴스 선택 -> Actions -> Security -> Modify IAM Role

```bash
# IAM Role 확인
curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/
aws s3 ls s3://your-bucket/
```

## 보안 체크리스트

- [ ] `.env`의 비밀번호를 강력하게 설정
- [ ] `.env`는 git에 커밋하지 않기 (.gitignore 확인)
- [ ] 보안 그룹: PostgreSQL 5432 포트는 필요한 IP만 개방
- [ ] IAM Role은 최소 권한 원칙 준수
