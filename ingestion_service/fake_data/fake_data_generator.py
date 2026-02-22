"""
더미 센서 데이터 생성 및 S3 적재

863호선 도로망 위에서 가상 차량 주행 데이터를 생성하고,
파싱 → 필터링 → Parquet 변환 → S3 업로드를 수행한다.

merge_size개 trip의 row를 모아 하나의 Parquet 파일로 병합 저장하여
S3 small file 문제를 방지한다.
"""

import io
import os
import sys
import json
import time
import csv
import math
import random
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

# ============================================================
# 설정
# ============================================================
S3_BUCKET = os.environ.get("S3_BUCKET")
S3_PREFIX = os.environ.get("S3_PREFIX", "raw-sensor-data")

# 대상 도로 bounding box
BBOXES = {
    "863": {
        "min_lon": 127.52, "max_lon": 127.78,
        "min_lat": 34.50, "max_lat": 35.07,
    },
}

# 데이터 유효 범위 (대한민국)
VALID_RANGE = {
    "lon": (124.0, 132.0),
    "lat": (33.0, 43.0),
    "spd": (0.0, 300.0),
    "acc": (-100.0, 100.0),
    "gyr": (-2000.0, 2000.0),
}

# Parquet 스키마
SCHEMA = pa.schema([
    ("timestamp", pa.int64()),
    ("trip_id", pa.string()),
    ("vehicle_id", pa.string()),
    ("accel_x", pa.float64()),
    ("accel_y", pa.float64()),
    ("accel_z", pa.float64()),
    ("gyro_x", pa.float64()),
    ("gyro_y", pa.float64()),
    ("gyro_z", pa.float64()),
    ("velocity", pa.float64()),
    ("lon", pa.float64()),
    ("lat", pa.float64()),
    ("hdop", pa.float64()),
    ("satellites", pa.int32()),
])

SAMPLING_INTERVAL_MS = 100
DEVICE_PROFILES = {
    "OBD2": {"acc_noise": 0.3, "gyr_noise": 0.08, "gps_noise": 0.00008},
    "DTG": {"acc_noise": 0.5, "gyr_noise": 0.12, "gps_noise": 0.00012},
}

s3 = boto3.client("s3")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")


# ============================================================
# 파싱 (lambda/parser.py 인라인)
# ============================================================
def parse_trip(raw: dict) -> tuple[str, str, str, list[dict]]:
    """trip JSON을 메타데이터와 parsed rows로 분리한다."""
    trip_id = raw["trip_id"]
    vehicle_id = raw["vin"]
    date = raw["start_time"][:10]

    rows = []
    for record in raw.get("records", []):
        parsed = _parse_record(record, trip_id, vehicle_id)
        if parsed is not None:
            rows.append(parsed)

    return trip_id, vehicle_id, date, rows


def _parse_record(record: dict, trip_id: str, vehicle_id: str) -> Optional[dict]:
    """단일 record를 flat row로 변환한다."""
    try:
        acc = record["acc"]
        gyr = record["gyr"]
        gps = record["gps"]

        row = {
            "timestamp": record["ts"],
            "trip_id": trip_id,
            "vehicle_id": vehicle_id,
            "accel_x": float(acc[0]),
            "accel_y": float(acc[1]),
            "accel_z": float(acc[2]),
            "gyro_x": float(gyr[0]),
            "gyro_y": float(gyr[1]),
            "gyro_z": float(gyr[2]),
            "velocity": float(record.get("spd", 0)),
            "lon": float(gps["lng"]),
            "lat": float(gps["lat"]),
            "hdop": float(gps.get("hdop", 99.0)),
            "satellites": int(gps.get("sat", 0)),
        }

        if not _is_valid(row):
            return None
        return row

    except (KeyError, IndexError, TypeError, ValueError):
        return None


def _is_valid(row: dict) -> bool:
    """row의 값이 유효 범위 안에 있는지 확인한다."""
    lon_min, lon_max = VALID_RANGE["lon"]
    lat_min, lat_max = VALID_RANGE["lat"]
    spd_min, spd_max = VALID_RANGE["spd"]
    acc_min, acc_max = VALID_RANGE["acc"]
    gyr_min, gyr_max = VALID_RANGE["gyr"]

    if not (lon_min <= row["lon"] <= lon_max):
        return False
    if not (lat_min <= row["lat"] <= lat_max):
        return False
    if not (spd_min <= row["velocity"] <= spd_max):
        return False
    for axis in ("accel_x", "accel_y", "accel_z"):
        if not (acc_min <= row[axis] <= acc_max):
            return False
    for axis in ("gyro_x", "gyro_y", "gyro_z"):
        if not (gyr_min <= row[axis] <= gyr_max):
            return False
    return True


# ============================================================
# 필터링 (lambda/filter.py 인라인)
# ============================================================
def filter_by_bbox(rows: list[dict]) -> list[dict]:
    """대상 도로 bounding box 안의 row만 필터링한다."""
    return [row for row in rows if _is_in_any_bbox(row["lon"], row["lat"])]


def _is_in_any_bbox(lon: float, lat: float) -> bool:
    for bbox in BBOXES.values():
        if (bbox["min_lon"] <= lon <= bbox["max_lon"]
                and bbox["min_lat"] <= lat <= bbox["max_lat"]):
            return True
    return False


# ============================================================
# 도로망 로드 + 경로 생성
# ============================================================
def load_road_network(csv_path: str) -> dict:
    """도로망 CSV에서 세그먼트 좌표 로드"""
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
    """도로망에서 랜덤 경로 선택"""
    target = int(random.lognormvariate(math.log(30), 0.5))
    target = max(min_segments, min(target, max_segments))

    link_ids = list(links.keys())
    start_link = random.choice(link_ids)
    route = []
    route.extend(links[start_link])

    visited = {start_link}
    current_end = (links[start_link][-1]["end_lon"], links[start_link][-1]["end_lat"])

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
        current_end = (links[best_link][-1]["end_lon"], links[best_link][-1]["end_lat"])

    if len(route) < min_segments:
        for lid in random.sample(link_ids, min(5, len(link_ids))):
            if lid not in visited:
                route.extend(links[lid])
                if len(route) >= min_segments:
                    break

    return route[:target]


# ============================================================
# 주행 데이터 생성
# ============================================================
def generate_speed_profile(num_points: int) -> list[float]:
    """현실적인 속도 프로파일 생성"""
    speeds = []
    current_speed = 0.0
    target_speed = random.uniform(50, 80)
    state = "accelerating"

    for i in range(num_points):
        if state == "accelerating":
            current_speed += random.uniform(1.0, 3.0)
            if current_speed >= target_speed:
                current_speed = target_speed
                state = "cruising"
        elif state == "cruising":
            current_speed += random.gauss(0, 1.5)
            current_speed = max(30, min(current_speed, 100))
            if random.random() < 0.03:
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
            if random.random() < 0.15:
                state = "accelerating"
                target_speed = random.uniform(50, 80)

        speeds.append(round(max(0, current_speed), 1))

    return speeds


def generate_pothole_impact(intensity: float = 1.0) -> list[dict]:
    """포트홀 충격 패턴 생성"""
    pattern = []
    main_impact = random.uniform(5, 15) * intensity
    pattern.append({"acc_z": main_impact, "gyr": random.uniform(1, 4) * intensity})
    pattern.append({"acc_z": main_impact * 0.6, "gyr": random.uniform(0.5, 2) * intensity})
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
    points_per_seg: int = 10,
    base_ts_ms: int = None,
) -> dict:
    """현실적인 주행 데이터 생성"""
    device_type = random.choice(list(DEVICE_PROFILES.keys()))
    profile = DEVICE_PROFILES[device_type]

    total_points = len(route) * points_per_seg
    speeds = generate_speed_profile(total_points)

    records = []
    ts_base = base_ts_ms if base_ts_ms is not None else int(time.time() * 1000)
    ts_offset = random.randint(0, 3600) * 1000
    ts = ts_base - ts_offset

    date_str = datetime.fromtimestamp(ts / 1000).strftime('%Y%m%d')
    trip_id = f"TRIP-{date_str}-{vehicle_id}-{int(ts / 1000) % 10000:04d}"

    impact_queue = []
    point_idx = 0

    for seg in route:
        for j in range(points_per_seg):
            if point_idx >= total_points:
                break

            t = j / points_per_seg
            lon = seg["start_lon"] + (seg["end_lon"] - seg["start_lon"]) * t
            lat = seg["start_lat"] + (seg["end_lat"] - seg["start_lat"]) * t

            lon += random.gauss(0, profile["gps_noise"])
            lat += random.gauss(0, profile["gps_noise"])

            speed = speeds[point_idx]
            speed_factor = speed / 60.0

            acc_x = random.gauss(0, profile["acc_noise"] * speed_factor)
            acc_y = random.gauss(0, profile["acc_noise"] * speed_factor)
            acc_z = 9.81 + random.gauss(0, profile["acc_noise"] * 0.5)
            gyr_x = random.gauss(0, profile["gyr_noise"] * speed_factor)
            gyr_y = random.gauss(0, profile["gyr_noise"] * speed_factor)
            gyr_z = random.gauss(0, profile["gyr_noise"] * speed_factor)

            if impact_queue:
                impact = impact_queue.pop(0)
                acc_z += impact["acc_z"]
                gyr_x += impact["gyr"]
                gyr_y += impact["gyr"] * random.uniform(0.3, 0.7)

            if j == 0:
                if random.random() < 0.05 and speed > 10:
                    intensity = random.uniform(0.5, 2.0)
                    impact_queue.extend(generate_pothole_impact(intensity))

            if random.random() < 0.005 and speed > 20:
                acc_z += random.uniform(3, 8)
                gyr_x += random.uniform(0.5, 1.5)

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

    duration_ms = len(records) * SAMPLING_INTERVAL_MS
    start_dt = time.strftime("%Y-%m-%dT%H:%M:%S+09:00", time.localtime(ts / 1000))
    end_dt = time.strftime("%Y-%m-%dT%H:%M:%S+09:00", time.localtime((ts + duration_ms) / 1000))

    return {
        "trip_id": trip_id,
        "vin": vehicle_id,
        "device_type": device_type,
        "start_time": start_dt,
        "end_time": end_dt,
        "records": records,
    }


# ============================================================
# 처리 + S3 업로드
# ============================================================
def process_trip(trip_json: dict) -> dict:
    """trip JSON → 파싱 → 필터링 → rows 반환 (S3 저장은 batch에서)"""
    try:
        trip_id, vehicle_id, date, rows = parse_trip(trip_json)

        if not rows:
            return {"status": "parse_failed", "trip_id": trip_id, "rows": [], "date": date}

        filtered_rows = filter_by_bbox(rows)

        if not filtered_rows:
            return {"status": "filtered_out", "trip_id": trip_id, "rows": [], "date": date}

        return {
            "status": "success",
            "trip_id": trip_id,
            "vehicle_id": vehicle_id,
            "date": date,
            "rows": filtered_rows,
        }

    except Exception as e:
        logger.error(f"Error processing trip: {e}")
        return {"status": "error", "error": str(e), "rows": [], "date": None}


def flush_buffer(buffer_rows: list[dict], date: str, batch_idx: int) -> str:
    """버퍼에 모인 rows를 하나의 Parquet로 병합하여 S3에 저장"""
    table = pa.Table.from_pylist(buffer_rows, schema=SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    key = f"{S3_PREFIX}/dt={date}/merged_{batch_idx:04d}.parquet"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())

    return key


def batch_generate_and_upload(
    num_trips: int,
    links: dict,
    start_date: datetime = None,
    end_date: datetime = None,
    merge_size: int = 2000,
):
    """배치로 더미 데이터 생성 및 S3 업로드 (merge_size개씩 병합)"""
    if start_date and end_date:
        logger.info(f"생성 시작: {num_trips}개 trip ({start_date.date()} ~ {end_date.date()}), {merge_size}개씩 병합")
        range_ms = int((end_date - start_date).total_seconds() * 1000)
    else:
        logger.info(f"생성 시작: {num_trips}개 trip, {merge_size}개씩 병합")
        range_ms = None

    success_count = 0
    skip_count = 0
    error_count = 0
    batch_idx = 0

    buffer_rows = []
    buffer_date = None

    start_time = time.time()

    for i in range(num_trips):
        vehicle_id = f"v{(i % 100):03d}"
        route = pick_route(links)

        base_ts_ms = None
        if start_date and range_ms:
            base_ts_ms = int(start_date.timestamp() * 1000) + random.randint(0, range_ms)

        trip_json = generate_trip(vehicle_id, route, points_per_seg=10, base_ts_ms=base_ts_ms)
        result = process_trip(trip_json)

        if result["status"] == "success":
            success_count += 1
            buffer_rows.extend(result["rows"])
            buffer_date = result["date"]

            if success_count % merge_size == 0 and buffer_rows:
                s3_key = flush_buffer(buffer_rows, buffer_date, batch_idx)
                logger.info(
                    f"[batch {batch_idx}] {len(buffer_rows)} rows → {s3_key.split('/')[-1]}"
                )
                buffer_rows = []
                batch_idx += 1
        else:
            if result["status"] == "filtered_out":
                skip_count += 1
            else:
                error_count += 1
                logger.warning(f"[{i+1:5d}] {result['status']}: {result.get('error', '')}")

        if (i + 1) % 500 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta_hours = (num_trips - i - 1) / rate / 3600 if rate > 0 else 0
            logger.info(f"  진행: {i+1}/{num_trips} | {rate:.1f} trips/sec | ETA: {eta_hours:.1f}h")

    # 남은 버퍼 flush
    if buffer_rows:
        s3_key = flush_buffer(buffer_rows, buffer_date, batch_idx)
        logger.info(f"[batch {batch_idx}] {len(buffer_rows)} rows → {s3_key.split('/')[-1]}")
        batch_idx += 1

    elapsed = time.time() - start_time
    logger.info(
        f"\n완료!\n"
        f"  성공: {success_count}/{num_trips}\n"
        f"  스킵: {skip_count}\n"
        f"  오류: {error_count}\n"
        f"  병합 파일: {batch_idx}개\n"
        f"  소요시간: {elapsed/3600:.1f}시간 ({elapsed/60:.0f}분)"
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="더미 센서 데이터 배치 생성 및 S3 적재")
    parser.add_argument("--trips", type=int, default=100, help="생성할 trip 수")
    parser.add_argument(
        "--road-network",
        type=str,
        default=str(Path(__file__).parent.parent.parent / "road_network_builder" / "output" / "road_network_863.csv"),
        help="도로망 CSV 경로",
    )
    parser.add_argument("--merge-size", type=int, default=2000, help="병합 단위 trip 수 (기본: 2000)")
    parser.add_argument("--test", action="store_true", help="테스트 모드 (데이터 생성만, S3 저장 X)")
    parser.add_argument("--date", type=str, default=None, help="날짜 (YYYY-MM-DD), 미지정 시 오늘")

    args = parser.parse_args()

    target = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = target
    end_date = target + timedelta(days=1)

    links = load_road_network(args.road_network)

    if args.test:
        logger.info("=== 테스트 모드: 데이터 생성 확인 ===")
        vehicle_id = "v001"
        route = pick_route(links)
        trip_json = generate_trip(vehicle_id, route, points_per_seg=10)

        logger.info(f"Trip ID: {trip_json['trip_id']}")
        logger.info(f"Vehicle ID: {trip_json['vin']}")
        logger.info(f"Start Time: {trip_json['start_time']}")
        logger.info(f"Records 수: {len(trip_json['records'])}")
        logger.info(f"Trip JSON 크기: {len(json.dumps(trip_json)) / 1024:.2f} KB")

        if trip_json['records']:
            logger.info(f"\n첫 번째 record 샘플:")
            logger.info(json.dumps(trip_json['records'][0], indent=2))

        logger.info("\n✓ 데이터 생성 성공!")

    else:
        batch_generate_and_upload(args.trips, links, start_date, end_date, merge_size=args.merge_size)
