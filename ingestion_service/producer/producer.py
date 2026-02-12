"""
테스트용 더미 센서 데이터 프로듀서

지방도 863호선 실제 도로망 좌표를 따라 가상 주행 데이터를 생성하여
Kinesis Data Streams에 전송한다.

현실적인 주행 환경을 시뮬레이션한다:
- 차량별 주행 시간이 다름 (5분 ~ 2시간)
- 100ms 간격 센서 측정 (실제 OBD2 기준)
- 속도에 따른 가속도/자이로 변화
- 과속방지턱, 급정거, GPS 음영 등 노이즈 이벤트
- 포트홀 통과 시 충격 패턴 (단발 충격 + 여진)

사용법:
    python producer.py                                          # 기본: 차량 1대
    python producer.py --vehicles 5                             # 차량 5대 동시 주행
    python producer.py --vehicles 3 --pothole-rate 0.2          # 포트홀 확률 20%
    python producer.py --pothole-segments seg1,seg2             # 특정 세그먼트에 포트홀 지정
    python producer.py --noise                                  # GPS 음영, 범위 밖 좌표 포함
    python producer.py --road-network path/to/road_network.csv  # 도로망 파일 지정
"""

import boto3
import json
import time
import csv
import math
import random
import argparse
import logging
from pathlib import Path
from config import STREAM_NAME

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

kinesis = boto3.client("kinesis", region_name="ap-northeast-2")


SAMPLING_INTERVAL_MS = 100  # 센서 측정 간격 (100ms)

# 범위 밖 좌표 (필터링 테스트용)
OUT_OF_RANGE_POINTS = [
    (126.90, 37.55),  # 서울
    (129.05, 35.15),  # 부산
    (128.60, 35.87),  # 대구
]

# 차량별 특성 (device_type별 노이즈 수준이 다름)
DEVICE_PROFILES = {
    "OBD2": {"acc_noise": 0.3, "gyr_noise": 0.08, "gps_noise": 0.00008},
    "DTG": {"acc_noise": 0.5, "gyr_noise": 0.12, "gps_noise": 0.00012},
}


def load_road_network(csv_path: str) -> dict:
    """도로망 CSV에서 링크별 세그먼트 좌표를 로드한다.

    Args:
        csv_path: road_network_863.csv 경로.

    Returns:
        {link_id: [seg_dict, ...]} 딕셔너리.
    """
    links = {}

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            lid = row["link_id"]
            if lid not in links:
                links[lid] = []
            links[lid].append({
                "s_id": row["s_id"],
                "start_lon": float(row["start_lon"]),
                "start_lat": float(row["start_lat"]),
                "end_lon": float(row["end_lon"]),
                "end_lat": float(row["end_lat"]),
            })

    logger.info(f"도로망 로드: {len(links)}개 링크, {sum(len(v) for v in links.values())}개 세그먼트")
    return links


def pick_route(links: dict, min_segments: int = 10, max_segments: int = 100) -> list[dict]:
    """도로망에서 랜덤 경로를 선택한다.

    차량마다 주행 거리가 다르므로 세그먼트 수를 랜덤으로 결정한다.

    Args:
        links: load_road_network()의 반환값.
        min_segments: 최소 세그먼트 수.
        max_segments: 최대 세그먼트 수.

    Returns:
        경로의 세그먼트 리스트.
    """
    # 주행 거리를 랜덤으로 결정 (짧은 주행이 더 많음)
    target = int(random.lognormvariate(math.log(30), 0.5))
    target = max(min_segments, min(target, max_segments))

    link_ids = list(links.keys())
    start_link = random.choice(link_ids)

    route = []
    route.extend(links[start_link])

    visited = {start_link}
    current_end = (
        links[start_link][-1]["end_lon"],
        links[start_link][-1]["end_lat"],
    )

    while len(route) < target:
        best_link = None
        best_dist = float("inf")

        for lid, segs in links.items():
            if lid in visited:
                continue
            start = (segs[0]["start_lon"], segs[0]["start_lat"])
            dist = (current_end[0] - start[0]) ** 2 + (current_end[1] - start[1]) ** 2
            if dist < best_dist:
                best_dist = dist
                best_link = lid

        if best_link is None or best_dist > 0.001:
            break

        visited.add(best_link)
        route.extend(links[best_link])
        current_end = (
            links[best_link][-1]["end_lon"],
            links[best_link][-1]["end_lat"],
        )

    if len(route) < min_segments:
        for lid in random.sample(link_ids, min(5, len(link_ids))):
            if lid not in visited:
                route.extend(links[lid])
                if len(route) >= min_segments:
                    break

    return route[:target]


def generate_speed_profile(num_points: int) -> list[float]:
    """현실적인 속도 프로파일을 생성한다.

    출발 시 가속 → 순항 → 신호/정지 → 재가속 패턴을 반복한다.

    Args:
        num_points: 데이터 포인트 수.

    Returns:
        속도 리스트 (km/h).
    """
    speeds = []
    current_speed = 0.0
    target_speed = random.uniform(50, 80)  # 순항 속도
    state = "accelerating"

    for i in range(num_points):
        if state == "accelerating":
            current_speed += random.uniform(1.0, 3.0)
            if current_speed >= target_speed:
                current_speed = target_speed
                state = "cruising"

        elif state == "cruising":
            current_speed += random.gauss(0, 1.5)  # 미세한 속도 변화
            current_speed = max(30, min(current_speed, 100))

            # 랜덤하게 감속 이벤트 발생
            if random.random() < 0.03:  # 3% 확률로 신호/커브
                state = "decelerating"
                target_speed = random.uniform(0, 30)

        elif state == "decelerating":
            current_speed -= random.uniform(2.0, 5.0)
            if current_speed <= target_speed:
                current_speed = max(0, target_speed)
                state = "stopped" if current_speed < 5 else "accelerating"
                target_speed = random.uniform(50, 80)

        elif state == "stopped":
            current_speed = 0
            # 정차 시간 (신호 대기)
            if random.random() < 0.15:  # 15% 확률로 출발
                state = "accelerating"
                target_speed = random.uniform(50, 80)

        speeds.append(round(max(0, current_speed), 1))

    return speeds


def generate_pothole_impact(intensity: float = 1.0) -> list[dict]:
    """포트홀 통과 시 충격 패턴을 생성한다.

    단발 충격 + 2~3회 여진 패턴.
    intensity로 포트홀 심각도를 조절한다.

    Args:
        intensity: 충격 강도 배수 (1.0 = 보통, 2.0 = 심각).

    Returns:
        충격 패턴 리스트 [{acc_z_offset, gyr_offset}, ...].
    """
    pattern = []

    # 주 충격 (1~2 포인트)
    main_impact = random.uniform(5, 15) * intensity
    pattern.append({"acc_z": main_impact, "gyr": random.uniform(1, 4) * intensity})
    pattern.append({"acc_z": main_impact * 0.6, "gyr": random.uniform(0.5, 2) * intensity})

    # 여진 (2~3 포인트, 감쇠)
    for j in range(random.randint(2, 3)):
        decay = 0.3 ** (j + 1)
        pattern.append({
            "acc_z": main_impact * decay,
            "gyr": random.uniform(0.2, 1) * intensity * decay,
        })

    return pattern


def generate_trip(
    vehicle_id: str,
    route: list[dict],
    pothole_rate: float,
    pothole_segments: set[str],
    include_noise: bool,
) -> dict:
    """경로를 따라 가상 주행 데이터를 생성한다.

    세그먼트당 여러 개의 센서 포인트를 생성하여
    실제 100ms 간격 측정을 시뮬레이션한다.

    Args:
        vehicle_id: 차량 식별자.
        route: 세그먼트 리스트.
        pothole_rate: 랜덤 포트홀 발생 확률.
        pothole_segments: 지정된 포트홀 세그먼트 s_id 집합.
        include_noise: True면 GPS 음영, 범위 밖 좌표 등 포함.

    Returns:
        trip JSON dict.
    """
    device_type = random.choice(list(DEVICE_PROFILES.keys()))
    profile = DEVICE_PROFILES[device_type]

    # 세그먼트당 측정 포인트 수 (세그먼트 길이 + 속도에 따라 다름)
    points_per_seg = random.randint(3, 8)
    total_points = len(route) * points_per_seg

    speeds = generate_speed_profile(total_points)

    records = []
    ts_base = int(time.time() * 1000)
    # 차량마다 출발 시간이 다름 (최근 1시간 내 랜덤)
    ts_offset = random.randint(0, 3600) * 1000
    ts = ts_base - ts_offset

    trip_id = f"TRIP-{time.strftime('%Y%m%d')}-{vehicle_id}-{int(time.time()) % 10000:04d}"

    # 포트홀 충격 패턴 큐
    impact_queue = []

    point_idx = 0
    for seg in route:
        # 세그먼트 내에서 선형 보간
        for j in range(points_per_seg):
            if point_idx >= total_points:
                break

            t = j / points_per_seg  # 0.0 ~ 1.0
            lon = seg["start_lon"] + (seg["end_lon"] - seg["start_lon"]) * t
            lat = seg["start_lat"] + (seg["end_lat"] - seg["start_lat"]) * t

            # GPS 노이즈
            lon += random.gauss(0, profile["gps_noise"])
            lat += random.gauss(0, profile["gps_noise"])

            speed = speeds[point_idx]

            # 기본 센서값 (속도에 비례하는 진동)
            speed_factor = speed / 60.0
            acc_x = random.gauss(0, profile["acc_noise"] * speed_factor)
            acc_y = random.gauss(0, profile["acc_noise"] * speed_factor)
            acc_z = 9.81 + random.gauss(0, profile["acc_noise"] * 0.5)
            gyr_x = random.gauss(0, profile["gyr_noise"] * speed_factor)
            gyr_y = random.gauss(0, profile["gyr_noise"] * speed_factor)
            gyr_z = random.gauss(0, profile["gyr_noise"] * speed_factor)

            # 포트홀 충격 적용
            if impact_queue:
                impact = impact_queue.pop(0)
                acc_z += impact["acc_z"]
                gyr_x += impact["gyr"]
                gyr_y += impact["gyr"] * random.uniform(0.3, 0.7)

            # 새로운 포트홀 감지
            if j == 0:  # 세그먼트 진입 시점에 판정
                is_pothole = seg["s_id"] in pothole_segments or random.random() < pothole_rate
                if is_pothole and speed > 10:  # 정차 중이면 충격 없음
                    intensity = random.uniform(0.5, 2.0)
                    impact_queue.extend(generate_pothole_impact(intensity))

            # 과속방지턱 (랜덤 발생)
            if random.random() < 0.005 and speed > 20:
                acc_z += random.uniform(3, 8)
                gyr_x += random.uniform(0.5, 1.5)

            # GPS 위성 수 (터널/음영 시뮬레이션)
            if include_noise and random.random() < 0.02:
                hdop = round(random.uniform(5.0, 20.0), 1)  # 정밀도 낮음
                satellites = random.randint(0, 3)
            else:
                hdop = round(random.uniform(0.8, 3.0), 1)
                satellites = random.randint(6, 12)

            records.append({
                "ts": ts + (point_idx * SAMPLING_INTERVAL_MS),
                "acc": [round(acc_x, 4), round(acc_y, 4), round(acc_z, 4)],
                "gyr": [round(gyr_x, 4), round(gyr_y, 4), round(gyr_z, 4)],
                "spd": speed,
                "gps": {
                    "lng": round(lon, 6),
                    "lat": round(lat, 6),
                    "hdop": hdop,
                    "sat": satellites,
                },
            })

            point_idx += 1

    # 범위 밖 좌표 삽입 (다른 도로 주행 후 863호선 진입 시뮬레이션)
    if include_noise:
        noise_count = random.randint(1, 5)
        for _ in range(noise_count):
            noise_lon, noise_lat = random.choice(OUT_OF_RANGE_POINTS)
            idx = random.randint(0, min(10, len(records)))  # 주행 초반에 삽입
            records.insert(idx, {
                "ts": ts - (random.randint(1, 100) * SAMPLING_INTERVAL_MS),
                "acc": [round(random.gauss(0, 0.5), 4) for _ in range(3)],
                "gyr": [round(random.gauss(0, 0.1), 4) for _ in range(3)],
                "spd": round(random.uniform(40, 80), 1),
                "gps": {
                    "lng": round(noise_lon + random.gauss(0, 0.001), 6),
                    "lat": round(noise_lat + random.gauss(0, 0.001), 6),
                    "hdop": round(random.uniform(1.0, 5.0), 1),
                    "sat": random.randint(4, 8),
                },
            })

    # 주행 시간 계산
    duration_ms = len(records) * SAMPLING_INTERVAL_MS
    start_dt = time.strftime("%Y-%m-%dT%H:%M:%S+09:00", time.localtime((ts) / 1000))
    end_dt = time.strftime("%Y-%m-%dT%H:%M:%S+09:00", time.localtime((ts + duration_ms) / 1000))

    return {
        "trip_id": trip_id,
        "vin": vehicle_id,
        "device_type": device_type,
        "start_time": start_dt,
        "end_time": end_dt,
        "records": records,
    }


def send(
    links: dict,
    vehicle_count: int,
    interval: int,
    pothole_rate: float,
    pothole_segments: set[str],
    include_noise: bool,
):
    """주기적으로 더미 데이터를 Kinesis에 전송한다.

    차량별로 다른 경로/속도/주행시간으로 데이터를 생성한다.
    """
    logger.info(
        f"프로듀서 시작: 차량 {vehicle_count}대, "
        f"{interval}초 간격, 포트홀 확률 {pothole_rate}, "
        f"지정 포트홀 세그먼트 {len(pothole_segments)}개"
    )

    count = 0
    try:
        while True:
            for i in range(vehicle_count):
                vid = f"v{i + 1:03d}"
                route = pick_route(links)
                trip = generate_trip(vid, route, pothole_rate, pothole_segments, include_noise)

                data = json.dumps(trip)
                data_size_kb = len(data.encode("utf-8")) / 1024

                kinesis.put_record(
                    StreamName=STREAM_NAME,
                    Data=data,
                    PartitionKey=vid,
                )

                duration_sec = len(trip["records"]) * SAMPLING_INTERVAL_MS / 1000
                count += 1
                logger.info(
                    f"[{count}] 전송: {trip['trip_id']}, "
                    f"device={trip['device_type']}, "
                    f"records={len(trip['records'])}, "
                    f"세그먼트={len(route)}, "
                    f"주행시간={duration_sec:.0f}초, "
                    f"크기={data_size_kb:.1f}KB"
                )

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info(f"프로듀서 종료. 총 {count}건 전송")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="포트홀 센서 데이터 더미 프로듀서")
    parser.add_argument("--vehicles", type=int, default=1, help="동시 주행 차량 수")
    parser.add_argument("--interval", type=int, default=10, help="전송 간격 (초)")
    parser.add_argument("--pothole-rate", type=float, default=0.1, help="랜덤 포트홀 확률 (0~1)")
    parser.add_argument("--pothole-segments", type=str, default="", help="포트홀 지정 세그먼트 (쉼표 구분)")
    parser.add_argument("--noise", action="store_true", help="GPS 음영, 범위 밖 좌표 포함")
    parser.add_argument(
        "--road-network",
        type=str,
        default=str(Path(__file__).parent / "road_network_863.csv"),
        help="도로망 CSV 경로",
    )
    args = parser.parse_args()

    pothole_segs = set(args.pothole_segments.split(",")) if args.pothole_segments else set()

    links = load_road_network(args.road_network)
    send(links, args.vehicles, args.interval, args.pothole_rate, pothole_segs, args.noise)