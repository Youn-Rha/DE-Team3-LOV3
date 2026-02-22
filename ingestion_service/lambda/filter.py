"""
도로 bounding box 필터링 모듈

파싱된 row 리스트에서 대상 도로 범위 안의 row만 남긴다.
"""

from config import BBOXES


def filter_by_bbox(rows: list[dict]) -> list[dict]:
    """대상 도로 bounding box 안의 row만 필터링한다.

    BBOXES에 등록된 도로 중 하나라도 범위 안이면 통과시킨다.
    모든 도로 범위 밖이면 드롭한다.

    Args:
        rows: 파싱된 row dict 리스트.

    Returns:
        필터링된 row dict 리스트.
    """
    return [row for row in rows if _is_in_any_bbox(row["lon"], row["lat"])]


def _is_in_any_bbox(lon: float, lat: float) -> bool:
    """좌표가 등록된 도로 bounding box 중 하나에 포함되는지 확인한다."""
    for bbox in BBOXES.values():
        if (
            bbox["min_lon"] <= lon <= bbox["max_lon"]
            and bbox["min_lat"] <= lat <= bbox["max_lat"]
        ):
            return True
    return False
