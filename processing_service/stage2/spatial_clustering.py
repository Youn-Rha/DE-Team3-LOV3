import yaml
import logging
import traceback
from scipy.spatial import cKDTree
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType


class PotholeSegmentProcessor:
    """
    S3 센서 데이터를 도로 세그먼트에 매핑하고, `is_pothole` 플래그를 사용하여
    포트홀 발생 횟수(`impact_count`)와 전체 데이터 포인트 수(`total_count`)를 집계하여
    S3에 Parquet 형식으로 저장하는 프로세서.

    Note:
        Input 데이터는 `stage1`에서 `is_pothole` 플래그(Boolean)가 계산되어 넘어오므로,
        `impact_score`를 직접 재평가하지 않고 `is_pothole` 플래그를 사용합니다.
    """

    def __init__(self, config_path="config.yaml"):
        """설정 로드 및 Spark 세션 초기화."""
        # 1. Config 로드
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
        except FileNotFoundError:
            logging.critical(f"Config file not found at: {config_path}")
            raise
        except yaml.YAMLError as e:
            logging.critical(f"Error parsing config file: {config_path}, Error: {e}")
            raise

        self.spark = SparkSession.builder \
            .appName(self.config['app']['name']) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.autoBroadcastJoinThreshold",
                    self.config['processing']['join_broadcast_limit'] * 1024 * 1024) \
            .getOrCreate()

        self.logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)

    def load_data(self):
        """S3 데이터 로드 및 도로망 중심점 계산."""
        self.logger.info("Loading Data from S3...")
        # stage1에서 is_pothole 플래그가 추가된 센서 데이터 로드
        self.sensor_df = self.spark.read.parquet(self.config['input']['sensor_data_path'])
        self.road_df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(self.config['input']['road_network_path'])

        # 도로 세그먼트 중심점 미리 계산
        self.road_centroids_df = self.road_df.withColumn(
           "centroid_lon", (F.col("start_lon") + F.col("end_lon")) / 2
        ).withColumn(
           "centroid_lat", (F.col("start_lat") + F.col("end_lat")) / 2
        ).select("s_id", "centroid_lon", "centroid_lat").distinct()

    def _prepare_spatial_index(self):
        """KDTree 생성 및 Broadcast (Spatial Join 준비)."""
        # Pandas로 변환 (도로 데이터가 크기에 따라 아키텍쳐 재설정)
        road_pdf = self.road_df.select("s_id", "start_lon", "start_lat", "end_lon", "end_lat").toPandas()

        # 중심점 계산
        road_pdf['mid_lon'] = (road_pdf['start_lon'] + road_pdf['end_lon']) / 2
        road_pdf['mid_lat'] = (road_pdf['start_lat'] + road_pdf['end_lat']) / 2

        # KDTree 생성
        coords = road_pdf[['mid_lon', 'mid_lat']].values
        tree = cKDTree(coords)

        # Broadcast
        self.bc_tree = self.spark.sparkContext.broadcast(tree)
        self.bc_road_ids = self.spark.sparkContext.broadcast(road_pdf['s_id'].values)

    def map_points_to_segments(self):
        """GPS 좌표를 가장 가까운 세그먼트 ID(s_id)에 매핑."""
        self._prepare_spatial_index()

        bc_tree = self.bc_tree
        bc_road_ids = self.bc_road_ids

        def find_nearest_segment(lon, lat):
            if lon is None or lat is None: return None
            # 가장 가까운 1개 점 탐색
            dist, idx = bc_tree.value.query([lon, lat], k=1)
            return str(bc_road_ids.value[idx])

        find_segment_udf = F.udf(find_nearest_segment, StringType())

        self.mapped_df = self.sensor_df.withColumn(
            "s_id",
            find_segment_udf(F.col("lon"), F.col("lat"))
        ).filter(F.col("s_id").isNotNull())

    def aggregate_metrics(self):
        """
        세그먼트별로 'impact_count' (포트홀 발생 횟수)와 'total_count' (전체 데이터 포인트 수)를 집계합니다.
        - impact_count: `is_pothole` 플래그가 true인 데이터 포인트의 수
        - total_count: 해당 세그먼트에 매핑된 모든 데이터 포인트의 수
        """
        # 집계 날짜 'date' 컬럼 추가 (처리 시점의 날짜 사용)
        df_with_date = self.mapped_df.withColumn("date", F.current_date())

        # is_pothole (Boolean)을 정수(0 또는 1)로 변환하여 합산
        self.aggregated_df = df_with_date.groupBy("s_id", "date").agg(
            F.sum(F.col("is_pothole").cast(IntegerType())).alias("impact_count"),
            F.count("*").alias("total_count")
        )

    def save_to_s3(self):
        """집계된 결과를 S3에 Parquet 형식으로 저장."""
        output_path = self.config['output']['s3_path']
        self.logger.info(f"Saving aggregated results to S3: {output_path}")

        # date별로 파티셔닝하여 저장
        self.result_df.write \
            .partitionBy("date") \
            .mode("overwrite") \
            .parquet(output_path)

    def run(self):
        self.logger.info("Starting Pothole Segment Aggregation Job")
        self.load_data()
        self.map_points_to_segments()
        self.aggregate_metrics()

        # 집계 결과와 도로 중심점 정보 조인
        self.result_df = self.aggregated_df.join(
            self.road_centroids_df,
            "s_id",
            "left"
        ).select( # 요구되는 컬럼 순서 및 이름으로 최종 DataFrame 구성
            "s_id",
            "centroid_lon",
            "centroid_lat",
            "date",
            "impact_count",
            "total_count"
        ).orderBy("date", "s_id") # 일관된 출력을 위해 정렬

        self.logger.info("--- Aggregated Results Preview (Top 20) ---")
        #self.result_df.show() # 테스트용 print
        #self.result_df.coalesce(1).write.option("header", "true").mode("overwrite").csv("data/output/pothole_results") # 테스트용 csv 저장
        self.logger.info(f"Total aggregated segments count: {self.result_df.count()}")

        self.save_to_s3()
        self.spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Stage 2: Spatial Clustering")
    parser.add_argument("--config-path", default="config.yaml",
                        help="config YAML 파일 경로 (기본: config.yaml)")
    args = parser.parse_args()

    processor = PotholeSegmentProcessor(config_path=args.config_path)
    processor.run()