"""
Pothole Loader - S3 Parquet -> PostgreSQL 적재

Stage 2 Spark 잡이 생성한 parquet 파일을 S3에서 읽어
PostgreSQL pothole_segments 테이블에 UPSERT하는 모듈.
"""

import os

import boto3
import pandas as pd
import requests
from sqlalchemy import text

from .base_loader import BaseLoader


class PotholeLoader(BaseLoader):
    """S3 parquet -> PostgreSQL 적재 엔진"""

    def load_from_s3(self, s3_path, execution_date):
        """
        S3에서 Stage 2 결과 parquet 읽기 및 적재

        Args:
            s3_path: S3 parquet 파일 경로 (s3://bucket/path/)
            execution_date: Airflow 실행 날짜 (YYYY-MM-DD)
        """
        try:
            self.logger.info(f"Loading parquet from S3: {s3_path}")

            # S3 경로 파싱
            s3_uri = s3_path.rstrip('/')
            if s3_uri.startswith('s3://'):
                s3_uri = s3_uri[5:]
            bucket, key = s3_uri.split('/', 1)

            s3_client = boto3.client('s3')

            # S3에서 parquet 파일 목록 가져오기
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)

            if 'Contents' not in response:
                raise FileNotFoundError(f"No files found in S3: {s3_path}")

            parquet_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]

            if not parquet_files:
                raise FileNotFoundError(f"No parquet files found in S3: {s3_path}")

            # 모든 parquet 파일을 읽어서 하나의 DataFrame으로 병합
            df_list = []
            for parquet_key in parquet_files:
                self.logger.info(f"Reading parquet file: s3://{bucket}/{parquet_key}")
                obj = s3_client.get_object(Bucket=bucket, Key=parquet_key)
                df_part = pd.read_parquet(obj['Body'])
                df_list.append(df_part)

            if not df_list:
                raise IOError(f"Failed to read parquet files from: {s3_path}")

            df = pd.concat(df_list, ignore_index=True)

            self.logger.info(f"Loaded {len(df)} records from S3 ({len(parquet_files)} files)")

            # date 컬럼 추가
            df["date"] = execution_date

            # 데이터 검증
            self._validate_dataframe(df)

            # PostgreSQL UPSERT
            self._upsert_to_postgresql(df)

            # 품질 체크
            self.run_quality_checks()

            # 역지오코딩으로 segment_address 갱신
            self._update_segment_addresses()

            self.logger.info("Data successfully loaded to PostgreSQL")
            return len(df)

        except Exception as e:
            self.logger.error(f"S3 load failed: {e}")
            raise

    def _validate_dataframe(self, df):
        """DataFrame 스키마 및 데이터 검증"""
        required_columns = {"s_id", "centroid_lon", "centroid_lat", "impact_count", "total_count", "date"}

        if not required_columns.issubset(df.columns):
            missing = required_columns - set(df.columns)
            raise ValueError(f"Missing columns: {missing}")

        # NULL 체크
        null_counts = df.isnull().sum()
        if null_counts.sum() > 0:
            self.logger.warning(f"Null values detected:\n{null_counts[null_counts > 0]}")

        # 데이터 타입 변환
        df["s_id"] = df["s_id"].astype(str)
        df["centroid_lon"] = pd.to_numeric(df["centroid_lon"], errors="coerce")
        df["centroid_lat"] = pd.to_numeric(df["centroid_lat"], errors="coerce")
        df["impact_count"] = df["impact_count"].astype(int)
        df["total_count"] = df["total_count"].astype(int)
        df["date"] = pd.to_datetime(df["date"]).dt.date

        self.logger.info("DataFrame validation passed")

    def _upsert_to_postgresql(self, df):
        """PostgreSQL에 UPSERT (멱등성 보장)"""
        try:
            batch_size = self.config["postgres"].get("batch_size", 1000)

            upsert_sql = text("""
                INSERT INTO pothole_segments (s_id, centroid_lon, centroid_lat, date, impact_count, total_count)
                VALUES (:s_id, :centroid_lon, :centroid_lat, :date, :impact_count, :total_count)
                ON CONFLICT (s_id, date) DO UPDATE SET
                    impact_count = EXCLUDED.impact_count,
                    total_count = EXCLUDED.total_count,
                    centroid_lon = EXCLUDED.centroid_lon,
                    centroid_lat = EXCLUDED.centroid_lat,
                    updated_at = CURRENT_TIMESTAMP
            """)

            records = df[["s_id", "centroid_lon", "centroid_lat", "date", "impact_count", "total_count"]].to_dict("records")

            with self.db_engine.connect() as conn:
                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    conn.execute(upsert_sql, batch)
                conn.commit()

            self.logger.info(f"Upserted {len(df)} rows (batch_size: {batch_size})")

        except Exception as e:
            self.logger.error(f"Upsert failed: {e}")
            raise

    def run_quality_checks(self):
        """데이터 품질 체크"""
        try:
            with self.db_engine.connect() as conn:
                # 1. impact_count > total_count 비정상 데이터 체크
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM pothole_segments
                    WHERE impact_count > total_count
                """))
                bad_count = result.scalar()
                if bad_count > 0:
                    self.logger.warning(
                        f"Quality issue: {bad_count} rows where impact_count > total_count"
                    )

                # 2. NULL s_id 체크
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM pothole_segments
                    WHERE s_id IS NULL OR s_id = ''
                """))
                null_sid = result.scalar()
                if null_sid > 0:
                    self.logger.warning(f"Quality issue: {null_sid} rows with NULL/empty s_id")

                # 3. 전일 대비 데이터량 변동 체크
                result = conn.execute(text("""
                    WITH daily_counts AS (
                        SELECT date, COUNT(*) AS cnt
                        FROM pothole_segments
                        GROUP BY date
                        ORDER BY date DESC
                        LIMIT 2
                    )
                    SELECT
                        MAX(CASE WHEN rn = 1 THEN cnt END) AS today_cnt,
                        MAX(CASE WHEN rn = 2 THEN cnt END) AS prev_cnt
                    FROM (
                        SELECT cnt, ROW_NUMBER() OVER (ORDER BY date DESC) AS rn
                        FROM daily_counts
                    ) t
                """))
                row = result.fetchone()
                if row and row[0] is not None and row[1] is not None:
                    today_cnt, prev_cnt = row[0], row[1]
                    if prev_cnt > 0:
                        change_ratio = abs(today_cnt - prev_cnt) / prev_cnt
                        if change_ratio > 0.5:
                            self.logger.warning(
                                f"Quality warning: data volume changed by "
                                f"{change_ratio:.0%} (today: {today_cnt}, prev: {prev_cnt})"
                            )

            self.logger.info("Quality checks completed")

        except Exception as e:
            self.logger.error(f"Quality check failed: {e}")

    def _update_segment_addresses(self):
        """신규 세그먼트에 대해 카카오 역지오코딩으로 주소 정보 갱신"""
        kakao_api_key = os.environ.get("KAKAO_REST_API_KEY")
        if not kakao_api_key:
            self.logger.warning("KAKAO_REST_API_KEY not set, skipping address update")
            return

        try:
            with self.db_engine.connect() as conn:
                # segment_address에 없는 신규 s_id 조회
                result = conn.execute(text("""
                    SELECT DISTINCT p.s_id, p.centroid_lon, p.centroid_lat
                    FROM pothole_segments p
                    LEFT JOIN segment_address sa ON p.s_id = sa.s_id
                    WHERE sa.s_id IS NULL
                """))
                new_segments = result.fetchall()

            if not new_segments:
                self.logger.info("No new segments to geocode")
                return

            self.logger.info(f"Geocoding {len(new_segments)} new segments")

            headers = {"Authorization": f"KakaoAK {kakao_api_key}"}
            upsert_sql = text("""
                INSERT INTO segment_address (s_id, road_name, district, centroid_lon, centroid_lat)
                VALUES (:s_id, :road_name, :district, :centroid_lon, :centroid_lat)
                ON CONFLICT (s_id) DO UPDATE SET
                    road_name = EXCLUDED.road_name,
                    district = EXCLUDED.district,
                    centroid_lon = EXCLUDED.centroid_lon,
                    centroid_lat = EXCLUDED.centroid_lat,
                    updated_at = CURRENT_TIMESTAMP
            """)

            geocoded = 0
            with self.db_engine.connect() as conn:
                for s_id, lon, lat in new_segments:
                    try:
                        resp = requests.get(
                            "https://dapi.kakao.com/v2/local/geo/coord2address.json",
                            params={"x": lon, "y": lat},
                            headers=headers,
                            timeout=5,
                        )
                        resp.raise_for_status()
                        data = resp.json()

                        road_name = None
                        district = None

                        if data.get("documents"):
                            doc = data["documents"][0]
                            road_addr = doc.get("road_address")
                            addr = doc.get("address")
                            if road_addr:
                                road_name = road_addr.get("road_name", "")
                                district = road_addr.get("region_2depth_name", "")
                            elif addr:
                                road_name = addr.get("region_3depth_name", "")
                                district = addr.get("region_2depth_name", "")

                        conn.execute(upsert_sql, {
                            "s_id": s_id,
                            "road_name": road_name,
                            "district": district,
                            "centroid_lon": lon,
                            "centroid_lat": lat,
                        })
                        geocoded += 1

                    except Exception as e:
                        self.logger.warning(f"Geocoding failed for {s_id}: {e}")
                        continue

                conn.commit()

            self.logger.info(f"Geocoded {geocoded}/{len(new_segments)} segments")

        except Exception as e:
            self.logger.error(f"Address update failed: {e}")


def load_stage2_results(s3_path, execution_date, config_path="config.yaml"):
    """
    Airflow DAG에서 호출하는 메인 함수

    Args:
        s3_path: S3 parquet 경로
        execution_date: 실행 날짜 (YYYY-MM-DD)
        config_path: 설정 파일 경로
    """
    loader = PotholeLoader(config_path)
    try:
        loader.load_from_s3(s3_path, execution_date)
    finally:
        loader.close()


if __name__ == "__main__":
    # 로컬 테스트용
    import argparse

    parser = argparse.ArgumentParser(description="Load Stage 2 results to PostgreSQL")
    parser.add_argument("--s3-path", required=True, help="S3 parquet path")
    parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
    parser.add_argument("--config", default="config.yaml", help="Config file path")

    args = parser.parse_args()
    load_stage2_results(args.s3_path, args.date, args.config)
