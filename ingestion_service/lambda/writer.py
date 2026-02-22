"""
Parquet 변환 + S3 적재 모듈

파싱/필터링된 row 리스트를 parquet로 변환하여
S3에 trip 단위 파티션으로 적재한다.
"""

import io
import boto3
import aioboto3
import pyarrow as pa
import pyarrow.parquet as pq

from config import BUCKET, PREFIX

s3 = boto3.client("s3")
async_session = aioboto3.Session()

# S3 적재 스키마 정의
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


def write_to_s3(rows: list[dict], date: str, trip_id: str) -> str:
    """parquet로 변환하여 S3에 적재한다.

    경로: s3://{bucket}/{prefix}/dt={date}/{trip_id}.parquet

    Args:
        rows: 파싱/필터링된 row dict 리스트.
        date: YYYY-MM-DD 문자열.
        trip_id: trip 고유 ID.

    Returns:
        적재된 S3 key 문자열.
    """
    table = pa.Table.from_pylist(rows, schema=SCHEMA)

    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    key = f"{PREFIX}/dt={date}/{trip_id}.parquet"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buffer.getvalue(),
    )

    return key


def _to_parquet_bytes(rows: list[dict]) -> bytes:
    """row 리스트를 parquet 바이트로 변환한다."""
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)
    return buffer.getvalue()


async def write_to_s3_async(rows: list[dict], date: str, trip_id: str) -> str:
    """비동기로 parquet 변환 후 S3에 적재한다.

    경로: s3://{bucket}/{prefix}/dt={date}/{trip_id}.parquet

    Args:
        rows: 파싱/필터링된 row dict 리스트.
        date: YYYY-MM-DD 문자열.
        trip_id: trip 고유 ID.

    Returns:
        적재된 S3 key 문자열.
    """
    key = f"{PREFIX}/dt={date}/{trip_id}.parquet"
    data = _to_parquet_bytes(rows)

    async with async_session.client("s3") as s3_client:
        await s3_client.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=data,
        )

    return key