"""
Stage 2: Spatial Clustering - 비즈니스 로직

Stage 1에서 필터링된 이상 데이터를 읽어, 도로 네트워크(세그먼트)에 매핑하고
세그먼트별 발생 횟수를 집계하여 Parquet로 저장하는 일 배치 단계입니다.

엔트리 포인트는 `run_job(spark, config, input_base_path, output_base_path, road_network_path, batch_date)` 이며,
`config_local.yaml` / `config_prod.yaml` (또는 config_stage2.yaml) 의 설정에 따라 입출력 경로와 Spark 튜닝 옵션이 결정됩니다.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
from scipy.spatial import cKDTree
from typing import Dict, Any
from datetime import date, timedelta
import os
import sys
import logging

logger = logging.getLogger(__name__)

STAGE2_OUTPUT_COLUMNS = [
    "s_id", "date", "impact_count", "total_count", "centroid_lon", "centroid_lat"
]

def _batch_date(batch_date_str: str = None) -> str:
    if batch_date_str and batch_date_str.strip():
        return batch_date_str.strip()
    env_date = os.getenv("BATCH_DATE", "").strip()
    if env_date:
        return env_date
    return (date.today() - timedelta(days=1)).isoformat()


class PotholeSegmentProcessor:
    """
    S3 센서 데이터를 도로 세그먼트에 매핑하고, 단순 발생 횟수를 집계하여
    S3에 Parquet 형식으로 저장하는 최적화된 프로세서.
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.config_broadcast = spark.sparkContext.broadcast(config)
        self.logger = logging.getLogger(__name__)

    def load_road_network(self, road_network_path: str) -> DataFrame:
        """도로 네트워크 데이터 로드"""
        # CSV 포맷 가정 (config에서 변경 가능하게 확장 가능)
        return self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(road_network_path)

    def get_road_centroids(self, road_df: DataFrame) -> DataFrame:
        """도로 세그먼트 중심점 계산"""
        return road_df.withColumn(
           "centroid_lon", (F.col("start_lon") + F.col("end_lon")) / 2
        ).withColumn(
           "centroid_lat", (F.col("start_lat") + F.col("end_lat")) / 2
        ).select("s_id", "centroid_lon", "centroid_lat").distinct()

    def _prepare_spatial_index(self, road_df: DataFrame):
        """KDTree 생성 및 Broadcast (Spatial Join 준비)."""
        # Pandas로 변환 (도로 데이터가 크기에 따라 아키텍쳐 재설정)
        road_pdf = road_df.select("s_id", "start_lon", "start_lat", "end_lon", "end_lat").toPandas()

        # 중심점 계산
        road_pdf['mid_lon'] = (road_pdf['start_lon'] + road_pdf['end_lon']) / 2
        road_pdf['mid_lat'] = (road_pdf['start_lat'] + road_pdf['end_lat']) / 2

        # KDTree 생성
        coords = road_pdf[['mid_lon', 'mid_lat']].values
        tree = cKDTree(coords)

        # Broadcast
        self.bc_tree = self.spark.sparkContext.broadcast(tree)
        self.bc_road_ids = self.spark.sparkContext.broadcast(road_pdf['s_id'].values)

    def map_points_to_segments(self, sensor_df: DataFrame, road_df: DataFrame) -> DataFrame:
        """GPS 좌표를 가장 가까운 세그먼트 ID(s_id)에 매핑."""
        self._prepare_spatial_index(road_df)

        bc_tree = self.bc_tree
        bc_road_ids = self.bc_road_ids

        def find_nearest_segment(lon, lat):
            if lon is None or lat is None: return None
            # 가장 가까운 1개 점 탐색
            dist, idx = bc_tree.value.query([lon, lat], k=1)
            return str(bc_road_ids.value[idx])

        find_segment_udf = F.udf(find_nearest_segment, StringType())

        return sensor_df.withColumn(
            "s_id",
            find_segment_udf(F.col("lon"), F.col("lat"))
        ).filter(F.col("s_id").isNotNull())

    def aggregate_metrics(self, mapped_df: DataFrame) -> DataFrame:
        """
        세그먼트별 'impact_count' (포트홀 발생 횟수)와 'total_count' (전체 데이터 포인트 수)를 집계합니다.
        - impact_count: `is_pothole` 플래그가 true인 데이터 포인트의 수
        - total_count: 해당 세그먼트에 매핑된 모든 데이터 포인트의 수
        """
        # 'date' 컬럼에 코드 실행 시점의 현재 날짜를 추가 (또는 파티션 날짜 사용 가능)
        df_with_date = mapped_df.withColumn("date", F.current_date())

        # is_pothole (Boolean)을 정수(0 또는 1)로 변환하여 합산
        return df_with_date.groupBy("s_id", "date").agg(
            F.sum(F.col("is_pothole").cast(IntegerType())).alias("impact_count"),
            F.count("*").alias("total_count")
        )


def run_job(
    spark: SparkSession,
    config: Dict[str, Any],
    input_base_path: str,
    output_base_path: str,
    road_network_path: str,
    batch_date: str = None,
) -> None:
    """
    Stage 2 일 배치 실행
    """
    batch_dt = _batch_date(batch_date)
    # 입력 경로는 Stage 1의 출력이므로 파티션이 있을 수 있음
    input_base_path = input_base_path.rstrip("/")
    output_base_path = output_base_path.rstrip("/")
    
    # 입력 파티션 경로 구성 (예: s3://.../dt=2026-02-19)
    partition_input = f"{input_base_path}/dt={batch_dt}"
    # 출력 파티션 경로 구성 (같은 날짜로 저장)
    partition_output = f"{output_base_path}/dt={batch_dt}"

    logger.info("배치 날짜: %s", batch_dt)
    logger.info("센서 입력: %s", partition_input)
    logger.info("도로 데이터: %s", road_network_path)
    logger.info("출력: %s", partition_output)

    processor = PotholeSegmentProcessor(spark, config)

    # 1. 데이터 로드
    try:
        sensor_df = spark.read.parquet(partition_input)
    except Exception as e:
        logger.error("입력 경로 읽기 실패 (파일 없음?): %s — 배치 스킵", partition_input)
        # Stage 1 결과가 없을 수도 있으므로 빈 DF 처리 혹은 에러
        raise RuntimeError(f"입력 데이터 없음: {partition_input}") from e

    road_df = processor.load_road_network(road_network_path)
    road_centroids_df = processor.get_road_centroids(road_df)

    # 2. 매핑 및 집계
    mapped_df = processor.map_points_to_segments(sensor_df, road_df)
    aggregated_df = processor.aggregate_metrics(mapped_df)
    
    # 3. 중심점 정보 조인 및 최종 컬럼 선택
    result_df = aggregated_df.join(
        road_centroids_df,
        "s_id",
        "left"
    ).select(STAGE2_OUTPUT_COLUMNS).orderBy("date", "s_id")

    # 4. 출력 파티션 수 최적화
    spark_config = config.get("spark", {})
    target_partitions = spark_config.get("coalesce_partitions", 16)
    current_partitions = result_df.rdd.getNumPartitions()

    if current_partitions > target_partitions:
        result_df = result_df.repartition(target_partitions)
        logger.info("출력 파티션 조정: %d → %d", current_partitions, target_partitions)

    # 5. 저장
    result_df = result_df.cache()

    # 터미널 결과 확인용 (최상위 20개)
    logger.info("--- Aggregated Results Preview (Top 20) ---")
    result_df.show(20, truncate=False)

    # 파티셔닝 없이 해당 날짜 폴더에 저장하거나, 필요시 partitionBy("date") 추가
    # 여기서는 partition_output 자체가 날짜 폴더이므로 직접 저장
    result_df.write.mode("overwrite").option("compression", "snappy").parquet(partition_output)
    logger.info("출력 저장 완료: %s", partition_output)

    # 6. 최종 통계
    final_count = result_df.count()
    result_df.unpersist()

    if final_count == 0:
        logger.warning("최종 결과가 비어 있음")
    else:
        logger.info("완료: 출력 %d 레코드 저장", final_count)


if __name__ == "__main__":
    import argparse

    # stage2 디렉터리를 sys.path에 추가 (connection.py import용)
    _stage2_dir = os.path.dirname(os.path.abspath(__file__))
    if _stage2_dir not in sys.path:
        sys.path.insert(0, _stage2_dir)

    # connection 모듈 임포트 (stage2 폴더 내 혹은 상위)
    try:
        from connection import load_config, get_spark_session
    except ImportError:
        # 상위 디렉토리에서 실행 시 처리
        sys.path.append(os.path.join(_stage2_dir, '..'))
        from stage2.connection import load_config, get_spark_session


    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(description="Stage 2: Spatial Clustering")
    parser.add_argument("--env", required=False, default="stage2", help="config file suffix (e.g. stage2 -> config_stage2.yaml)")
    parser.add_argument("--batch-date", default=None, help="YYYY-MM-DD")
    args = parser.parse_args()

    # config 파일 로드
    # 예: --env prod -> config_prod.yaml, --env stage2 -> config_stage2.yaml
    config_filename = f"config_{args.env}.yaml"
    config_path = os.path.join(_stage2_dir, config_filename)
    
    if not os.path.isfile(config_path):
        logger.error("설정 파일 없음: %s", config_path)
        sys.exit(1)

    config = load_config(config_path)
    storage = config.get("storage", {})
    
    # Config에서 경로 읽기
    input_base = storage.get("input_base_path")
    output_base = storage.get("output_base_path")
    road_network = storage.get("road_network_path")

    if not input_base or not output_base or not road_network:
         logger.error("필수 스토리지 설정 누락 (input_base_path, output_base_path, road_network_path)")
         sys.exit(1)

    spark = get_spark_session(config)
    try:
        run_job(spark, config, input_base, output_base, road_network, args.batch_date)
    finally:
        spark.stop()
