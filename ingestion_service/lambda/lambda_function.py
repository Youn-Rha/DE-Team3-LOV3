"""
Ingestion Lambda 핸들러

Kinesis Data Streams에서 차량 센서 JSON을 받아서
파싱 → 필터링 → parquet 변환 → S3 적재를 수행한다.

트리거: Kinesis Data Streams (이벤트 소스 매핑)
출력: S3 parquet 파일 (trip 단위 파티션)
"""

import json
import base64
import logging
import asyncio

from parser import parse_trip
from filter import filter_by_bbox
from writer import write_to_s3_async

logger = logging.getLogger()
logger.setLevel(logging.INFO)

MAX_CONCURRENT = 10  # S3 동시 적재 수 제한


async def _process_records(records):
    """Kinesis 레코드를 파싱/필터링 후 비동기로 S3에 적재한다.

    Args:
        records: Kinesis event의 Records 배열.

    Returns:
        처리 결과 요약 dict.
    """
    results = {
        "total": 0,
        "processed": 0,
        "filtered_out": 0,
        "errors": 0,
    }

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    tasks = []

    for record in records:
        results["total"] += 1

        try:
            # Kinesis 레코드 디코딩
            payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            raw = json.loads(payload)

            # 1. 파싱
            trip_id, vehicle_id, date, rows = parse_trip(raw)

            if not rows:
                logger.warning(f"파싱 결과 없음: trip_id={trip_id}")
                results["errors"] += 1
                continue

            # 2. 필터링
            filtered = filter_by_bbox(rows)

            if not filtered:
                logger.info(f"대상 도로 밖: trip_id={trip_id}, 원본={len(rows)}건")
                results["filtered_out"] += 1
                continue

            # 3. S3 적재 task 등록
            tasks.append(
                _write_with_limit(semaphore, filtered, date, trip_id, results)
            )

        except Exception as e:
            logger.error(f"처리 실패: {e}", exc_info=True)
            results["errors"] += 1

    # 모든 S3 적재를 동시 실행
    if tasks:
        await asyncio.gather(*tasks)

    return results


async def _write_with_limit(semaphore, rows, date, trip_id, results):
    """동시 실행 수를 제한하여 S3에 적재한다.

    Args:
        semaphore: 동시 실행 제한 세마포어.
        rows: 파싱/필터링된 row 리스트.
        date: YYYY-MM-DD 문자열.
        trip_id: trip 고유 ID.
        results: 처리 결과 카운터 dict (뮤터블 공유).
    """
    async with semaphore:
        try:
            key = await write_to_s3_async(rows, date, trip_id)
            logger.info(f"적재 완료: trip_id={trip_id}, 적재={len(rows)}건, key={key}")
            results["processed"] += 1
        except Exception as e:
            logger.error(f"S3 적재 실패: trip_id={trip_id}, {e}", exc_info=True)
            results["errors"] += 1


def lambda_handler(event, context):
    """Kinesis 이벤트를 처리한다.

    Kinesis에서 배치로 전달된 레코드를 파싱 → 필터링 후
    비동기로 S3에 동시 적재한다.

    Args:
        event: Kinesis 이벤트. Records[] 배열 포함.
        context: Lambda 실행 컨텍스트.

    Returns:
        처리 결과 요약 dict.
    """
    records = event.get("Records", [])
    results = asyncio.run(_process_records(records))

    logger.info(f"배치 처리 완료: {results}")
    return results