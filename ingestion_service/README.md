# Ingestion Service

863호선 도로망 위에서 가상 차량 센서 데이터를 생성하고, Parquet으로 변환하여 S3에 적재합니다. Spark 처리 파이프라인(processing_service)의 입력 데이터를 만드는 역할입니다.

## 역할

실제 차량 센서 데이터 대신, 현실적인 주행 패턴을 시뮬레이션하여 파이프라인 검증용 데이터를 생성합니다. 생성된 데이터는 S3에 날짜별 파티션으로 적재되어 Airflow DAG이 배치 처리할 수 있는 형태로 준비됩니다.

## 입출력

| | 항목 | 설명 |
|---|---|---|
| **Input** | `road_network_863.csv` | 863호선 도로망 세그먼트 좌표 (road_network_builder 출력) |
| **Output** | S3 `raw-sensor-data/dt={날짜}/` | 차량 센서 데이터 (Parquet, Snappy 압축) |

## 데이터 생성 파이프라인

```
도로망 CSV 로드
  → 랜덤 경로 선택 (10~100개 세그먼트)
  → 주행 데이터 생성 (가속도, 자이로, GPS, 속도)
  → 파싱 (중첩 JSON → flat row)
  → 필터링 (863호선 bounding box 범위 내만)
  → merge_size개 trip 병합 → Parquet → S3
```

### 병합 저장

trip마다 개별 Parquet 파일을 만들면 S3에 수만 개의 small file이 생성되어 Spark 읽기 성능이 저하됩니다. `merge_size`개 trip의 row를 메모리에 모아 하나의 Parquet 파일로 병합 저장합니다.

```
# merge_size=2000 기준
50,000 trips → 25개 merged Parquet 파일
```

## S3 적재 구조

```
s3://{bucket}/raw-sensor-data/
  └── dt=2026-02-22/
      ├── merged_0000.parquet
      ├── merged_0001.parquet
      └── ...
```

`dt=YYYY-MM-DD/` 파티션으로 Spark가 특정 날짜만 읽을 수 있습니다.

## Parquet 스키마

| 컬럼 | 타입 | 설명 |
|------|------|------|
| timestamp | INT64 | 측정 시각 (epoch ms) |
| trip_id | STRING | 주행 고유 ID |
| vehicle_id | STRING | 차량 식별자 |
| accel_x | DOUBLE | X축 가속도 (m/s²) |
| accel_y | DOUBLE | Y축 가속도 |
| accel_z | DOUBLE | Z축 가속도 (포트홀 탐지 핵심) |
| gyro_x | DOUBLE | X축 자이로 (°/s) |
| gyro_y | DOUBLE | Y축 자이로 |
| gyro_z | DOUBLE | Z축 자이로 |
| velocity | DOUBLE | 주행 속도 (km/h) |
| lon | DOUBLE | 경도 |
| lat | DOUBLE | 위도 |
| hdop | DOUBLE | GPS 수평 정밀도 |
| satellites | INT32 | 수신 위성 수 |

## 시뮬레이션 패턴

생성 데이터가 실제 차량 센서 데이터와 유사하도록 다음 패턴을 구현합니다.

| 패턴 | 설명 |
|------|------|
| 속도 프로파일 | 가속 → 순항 → 감속 → 정차 → 재가속 (상태 머신) |
| 포트홀 충격 | 세그먼트당 5% 확률, 주 충격 + 여진 감쇠 패턴 |
| 과속방지턱 | 0.5% 확률, Z축 급격한 단발 충격 |
| 디바이스 노이즈 | OBD2 / DTG 단말기별 센서 노이즈 차이 |
| GPS 정밀도 | hdop, 위성 수 랜덤 변동 |

## 구조

```
ingestion_service/
└── fake_data/
    ├── fake_data_generator.py   # 데이터 생성 + 파싱 + 필터링 + S3 적재
    └── requirements.txt
```

## 실행 방법

```bash
cd ingestion_service/fake_data
pip install -r requirements.txt

# 테스트 (S3 저장 없이 데이터 생성만 확인)
python fake_data_generator.py --test --trips 1

# 특정 날짜 데이터 생성 (S3 적재)
S3_BUCKET=your-bucket python fake_data_generator.py \
    --trips 50000 \
    --date 2026-02-22 \
    --merge-size 2000

# 도로망 CSV 직접 지정
python fake_data_generator.py \
    --road-network /path/to/road_network_863.csv \
    --trips 1000
```

## 필요 환경변수

| 변수 | 설명 | 기본값 |
|------|------|--------|
| `S3_BUCKET` | 데이터 적재 S3 버킷명 | (필수) |
| `S3_PREFIX` | S3 키 프리픽스 | `raw-sensor-data` |
