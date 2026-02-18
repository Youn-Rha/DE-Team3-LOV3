"""
Complaint Loader - 공공데이터 포트홀 민원 API -> PostgreSQL 적재

공공데이터 포트홀 민원(위경도) API를 호출하고,
863호선 road_network 기준 cKDTree로 가장 가까운 세그먼트를 매칭하여
임계거리(500m) 이내 민원만 pothole_complaints 테이블에 적재.
"""

import argparse
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from scipy.spatial import cKDTree
from sqlalchemy import text

from .base_loader import BaseLoader


class ComplaintLoader(BaseLoader):
    """공공데이터 포트홀 민원 API -> 863호선 필터 -> PostgreSQL 적재"""

    def __init__(self, config_path, road_network_path):
        super().__init__(config_path)
        self.api_config = self.config["complaint_api"]
        self.max_distance_m = self.api_config.get("max_distance_m", 500)
        self._build_spatial_index(road_network_path)

    def _build_spatial_index(self, road_network_path):
        """road_network_863.csv 로드 -> 세그먼트 중심점 -> cKDTree 구축"""
        road_df = pd.read_csv(road_network_path)
        road_df["mid_lon"] = (road_df["start_lon"] + road_df["end_lon"]) / 2
        road_df["mid_lat"] = (road_df["start_lat"] + road_df["end_lat"]) / 2

        # 중복 s_id 제거 (세그먼트별 중심점 평균)
        centroids = road_df.groupby("s_id")[["mid_lon", "mid_lat"]].mean().reset_index()

        self.segment_ids = centroids["s_id"].values
        coords = centroids[["mid_lon", "mid_lat"]].values
        self.tree = cKDTree(coords)

        self.logger.info(f"Spatial index built: {len(centroids)} segments from {road_network_path}")

    def load_complaints(self, service_key, date_from, date_to):
        """
        민원 데이터 로드 메인 플로우

        Args:
            service_key: 공공데이터 API 인증키
            date_from: 조회 시작일 (yyyyMMdd)
            date_to: 조회 종료일 (yyyyMMdd)
        """
        # 1. API 호출 (15일 단위 분할)
        records = self._call_api_paginated(service_key, date_from, date_to)
        if not records:
            self.logger.warning("No records returned from API")
            return 0

        df = pd.DataFrame(records)
        self.logger.info(f"API returned {len(df)} total records")

        # 2. 863호선 필터링
        df = self._filter_by_road_network(df)
        if df.empty:
            self.logger.warning("No records within road network range")
            return 0

        self.logger.info(f"{len(df)} records within {self.max_distance_m}m of 863 route")

        # 3. DB 적재
        self._insert_to_db(df)
        return len(df)

    def _call_api_paginated(self, service_key, date_from, date_to):
        """15일 단위로 API를 분할 호출"""
        all_records = []
        start = datetime.strptime(date_from, "%Y%m%d")
        end = datetime.strptime(date_to, "%Y%m%d")

        while start <= end:
            chunk_end = min(start + timedelta(days=14), end)
            chunk_from = start.strftime("%Y%m%d")
            chunk_to = chunk_end.strftime("%Y%m%d")

            records = self._call_api(service_key, chunk_from, chunk_to)
            all_records.extend(records)

            start = chunk_end + timedelta(days=1)

        return all_records

    def _call_api(self, service_key, date_from, date_to):
        """공공데이터 API 단일 호출"""
        base_url = self.api_config["base_url"]
        endpoint = self.api_config["endpoint"]
        url = f"{base_url}{endpoint}"

        all_items = []
        page_no = 1
        num_of_rows = 1000

        while True:
            params = {
                "serviceKey": service_key,
                "dateFrom": date_from,
                "dateTo": date_to,
                "pageNo": page_no,
                "numOfRows": num_of_rows,
                "type": "json",
            }

            try:
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                self.logger.error(f"API call failed (page {page_no}): {e}")
                break

            # Support both {"response": {"body": ...}} and {"body": ...} structures
            if "response" in data:
                body = data["response"].get("body", {})
            else:
                body = data.get("body", {})
                if not body:
                    self.logger.warning(f"Unexpected response structure, top-level keys: {list(data.keys())}")

            items = body.get("items", [])

            if not items:
                self.logger.warning(f"No items found. body keys: {list(body.keys()) if body else 'empty'}, totalCount: {body.get('totalCount')}")
                break

            for entry in items:
                # API returns each element as {"item": {...}}
                item = entry.get("item", entry)
                try:
                    all_items.append({
                        "create_dt": pd.to_datetime(str(item["create_dt"])).date(),
                        "event_lat": float(item["event_lat"]),
                        "event_lon": float(item["event_lon"]),
                    })
                except (KeyError, ValueError) as e:
                    self.logger.warning(f"Skipping malformed record: {e}")
                    continue

            total_count = body.get("totalCount", 0)
            if page_no * num_of_rows >= total_count:
                break
            page_no += 1

        self.logger.info(f"API [{date_from}~{date_to}]: {len(all_items)} records")
        return all_items

    def _filter_by_road_network(self, df):
        """cKDTree로 nearest_s_id + distance_m 계산, 임계거리 초과 제거"""
        coords = df[["event_lon", "event_lat"]].values
        distances, indices = self.tree.query(coords)

        # 위경도 거리 -> 미터 근사 변환 (위도 37도 기준: 1도 ≈ 111km)
        lon_scale = np.cos(np.radians(35.5)) * 111_000  # 863호선 위도 ~35.5
        lat_scale = 111_000

        # 실제 미터 거리 계산
        tree_coords = np.array([
            [self.tree.data[idx][0], self.tree.data[idx][1]] for idx in indices
        ])
        d_lon = (coords[:, 0] - tree_coords[:, 0]) * lon_scale
        d_lat = (coords[:, 1] - tree_coords[:, 1]) * lat_scale
        distance_m = np.sqrt(d_lon**2 + d_lat**2)

        df = df.copy()
        df["nearest_s_id"] = [str(self.segment_ids[i]) for i in indices]
        df["distance_m"] = distance_m

        # 임계거리 필터
        filtered = df[df["distance_m"] <= self.max_distance_m].copy()
        return filtered

    def _insert_to_db(self, df):
        """pothole_complaints 테이블에 INSERT"""
        batch_size = self.config["postgres"].get("batch_size", 1000)

        insert_sql = text("""
            INSERT INTO pothole_complaints (create_dt, event_lat, event_lon, nearest_s_id, distance_m)
            VALUES (:create_dt, :event_lat, :event_lon, :nearest_s_id, :distance_m)
            ON CONFLICT (create_dt, event_lat, event_lon) DO UPDATE SET
                nearest_s_id = EXCLUDED.nearest_s_id,
                distance_m = EXCLUDED.distance_m
        """)

        records = df[["create_dt", "event_lat", "event_lon", "nearest_s_id", "distance_m"]].to_dict("records")

        with self.db_engine.connect() as conn:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                conn.execute(insert_sql, batch)
            conn.commit()

        self.logger.info(f"Inserted {len(records)} complaint records")


def load_complaints(service_key, date_from, date_to, road_network_path, config_path="config.yaml"):
    """외부에서 호출하는 메인 함수"""
    loader = ComplaintLoader(config_path, road_network_path)
    try:
        return loader.load_complaints(service_key, date_from, date_to)
    finally:
        loader.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load pothole complaint data from public API")
    parser.add_argument("--service-key", required=True, help="공공데이터 API 인증키")
    parser.add_argument("--date-from", required=True, help="조회 시작일 (yyyyMMdd)")
    parser.add_argument("--date-to", required=True, help="조회 종료일 (yyyyMMdd)")
    parser.add_argument("--road-network", required=True, help="road_network_863.csv 경로")
    parser.add_argument("--config", default="config.yaml", help="Config file path")

    args = parser.parse_args()
    count = load_complaints(args.service_key, args.date_from, args.date_to, args.road_network, args.config)
    print(f"Loaded {count} complaint records")
