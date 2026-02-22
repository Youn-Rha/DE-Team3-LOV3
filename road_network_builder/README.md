# Road Network Builder

ITS 표준 노드링크 shapefile을 가공하여 포트홀 탐지 파이프라인의 **공간 기준 데이터**를 생성하는 모듈입니다.

## 역할

파이프라인에서 GPS 좌표를 도로 위의 특정 구간으로 매핑하려면, 도로를 일정한 단위로 나눈 세그먼트 기준 데이터가 필요합니다. 이 모듈은 두 가지 산출물을 생성합니다.

- **도로 세그먼트 (`build_segments.py`)** — 도로 링크를 50m 단위로 분할하여 세그먼트 ID와 시작/끝 좌표를 생성
- **도로 위험도 등급 (`build_road_grade.py`)** — 도로 속성(등급, 차로수, 제한속도, 유형)을 점수화하여 세그먼트별 1~5등급 부여

두 산출물 모두 한 번만 실행하면 되는 정적 참조 데이터입니다.

## 원본 데이터

[ITS 국가교통정보센터](https://www.its.go.kr/nodelink/nodelinkRef)에서 제공하는 **전국 표준노드링크** 데이터를 사용합니다.

표준노드링크는 전국 도로를 노드(교차점)와 링크(도로 구간)로 모델링한 GIS 데이터로, Shapefile 형식으로 배포됩니다. 각 링크에는 다음과 같은 도로 속성이 포함되어 있습니다.

| 필드 | 설명 | 예시 |
|------|------|------|
| `LINK_ID` | 링크 고유 ID | `3250010503` |
| `ROAD_NO` | 노선번호 | `863` (지방도 863호선) |
| `ROAD_RANK` | 도로등급 코드 | `103`(국도), `106`(지방도), `108`(군도) |
| `LANES` | 차로수 | `2`, `4` |
| `MAX_SPD` | 최고제한속도 (km/h) | `60`, `80` |
| `ROAD_TYPE` | 도로유형 코드 | `0`(기타), `2`(터널), `3`(고가도로) |
| `geometry` | 도로 형상 (LineString) | EPSG:5179 좌표계 |

이 중 `ROAD_NO=863` (여수공단 지방도 863호선)으로 필터링하여 **740개 링크**를 추출합니다.

## 파이프라인에서의 활용

생성된 데이터는 다운스트림에서 다음과 같이 사용됩니다.

| 산출물 | 사용처 | 용도 |
|--------|--------|------|
| `road_network_863.csv` | Processing Stage 2 (Spark) | cKDTree 공간 인덱스를 구축하여 GPS 좌표 → 최근접 세그먼트 매핑 |
| `road_network_863.csv` | Serving (complaint_loader) | 민원 위치 → 최근접 세그먼트 매핑 (500m 이내) |
| `road_grade_863.csv` | Serving (PostgreSQL) | 대시보드 보수 우선순위 산정 시 도로 위험도 가중치로 활용 |

## 입출력

### build_segments.py

| | 항목 | 설명 |
|---|---|---|
| **Input** | ITS shapefile (`MOCT_LINK.shp`) | 전국 표준노드링크 (EPSG:5179) |
| **Output** | `road_network_863.csv` | 3,535개 세그먼트 |

**Output 스키마:**

| 컬럼 | 설명 |
|------|------|
| `s_id` | 세그먼트 ID (`{링크ID}_{번호}`) |
| `link_id` | 원본 링크 ID |
| `start_lon`, `start_lat` | 세그먼트 시작점 (WGS84) |
| `end_lon`, `end_lat` | 세그먼트 끝점 (WGS84) |

### build_road_grade.py

| | 항목 | 설명 |
|---|---|---|
| **Input** | ITS shapefile + 세그먼트 CSV | 동일 shapefile에서 도로 속성 추출 |
| **Output** | `road_grade_863.csv` | 세그먼트별 위험도 등급 |

**Output 스키마:**

| 컬럼 | 설명 |
|------|------|
| `s_id` | 세그먼트 ID |
| `link_id` | 원본 링크 ID |
| `road_grade` | 위험도 등급 (1=안전, 5=위험) |

## 핵심 설계

### 50m 세그먼트 분할

하나의 도로 링크는 수백 미터에서 수 킬로미터까지 길이가 다양합니다. 포트홀 위치를 정밀하게 특정하려면 균일한 길이의 공간 단위가 필요하기 때문에, 링크를 **50m 간격으로 분할**했습니다.

- 미터 좌표계(EPSG:5186)로 변환 후 Shapely `substring`으로 정밀 분할
- 분할 후 WGS84(EPSG:4326)로 변환하여 GPS 좌표와 매칭 가능하게 출력
- 863호선 기준: 740개 링크 → **3,535개 세그먼트**

### 도로 위험도 등급 산출

도로의 물리적 특성만으로도 사고 위험도를 추정할 수 있습니다. ITS 링크 속성 4가지를 각각 1~5점으로 점수화한 뒤 가중합산합니다.

| 요소 | 필드 | 가중치 | 점수 기준 |
|------|------|--------|----------|
| 도로등급 | `ROAD_RANK` | 30% | 고속도로(1) ~ 군도·구도(5) |
| 차로수 | `LANES` | 25% | 4+차로(1), 2~3차로(3), 1차로(5) |
| 제한속도 | `MAX_SPD` | 25% | 20이하(1), 40(2), 60(3), 80(4), 100+(5) |
| 도로유형 | `ROAD_TYPE` | 20% | 교량(1) ~ 고가도로(5) |

가중합산 점수를 `pd.cut`으로 5등분하여 최종 등급을 부여합니다. 같은 링크에 속한 세그먼트는 동일한 등급을 가집니다.

## 구조

```
road_network_builder/
├── build_segments.py       # 도로 세그먼트 분할
├── build_road_grade.py     # 도로 위험도 등급 산출
├── config.yaml.example     # 설정 파일 템플릿
└── requirements.txt
```

## 실행 방법

```bash
cp config.yaml.example config.yaml   # 설정 파일 생성 후 경로 수정
pip install -r requirements.txt

python build_segments.py              # 세그먼트 CSV 생성
python build_road_grade.py            # 위험도 등급 CSV 생성
```
