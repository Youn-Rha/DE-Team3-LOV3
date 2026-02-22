"""
Road Grade Builder

ITS 표준 노드링크 shapefile의 도로 속성(ROAD_RANK, LANES, MAX_SPD, ROAD_TYPE)을
점수화하여 세그먼트별 도로 위험도 등급(1~5)을 산출합니다.

사용법:
    python build_road_grade.py                  # config.yaml 사용
    python build_road_grade.py custom.yaml      # 커스텀 설정 파일 사용

출력 스키마:
    s_id        - 세그먼트 ID
    link_id     - 원본 링크 ID
    road_grade  - 도로 위험도 등급 (1~5, 5=가장 위험)
"""

import os
import sys

import pandas as pd

from build_segments import load_config, load_and_filter


# --- 점수 매핑 ---

ROAD_RANK_SCORE = {
    "101": 1,  # 고속도로
    "102": 1,  # 도시고속도로
    "103": 2,  # 국도
    "104": 2,  # 국가지원지방도
    "105": 3,  # 특별·광역시도
    "106": 3,  # 지방도
    "107": 4,  # 시도
    "108": 5,  # 군도
    "109": 5,  # 구도
}

ROAD_TYPE_SCORE = {
    "0": 3,   # 기타
    "1": 1,   # 교량
    "2": 4,   # 터널
    "3": 5,   # 고가도로
    "4": 2,   # 지하도로
    "5": 3,   # 일반도로
}


def score_road_rank(val):
    return ROAD_RANK_SCORE.get(str(val).strip(), 3)


def score_lanes(val):
    try:
        lanes = int(val)
    except (ValueError, TypeError):
        return 3
    if lanes >= 4:
        return 1
    if lanes >= 2:
        return 3
    return 5


def score_max_spd(val):
    try:
        spd = int(val)
    except (ValueError, TypeError):
        return 3
    if spd >= 100:
        return 5
    if spd >= 80:
        return 4
    if spd >= 60:
        return 3
    if spd >= 40:
        return 2
    return 1


def score_road_type(val):
    return ROAD_TYPE_SCORE.get(str(val).strip(), 3)


def compute_weighted_score(df):
    """각 요소를 점수화하고 가중합산한다."""
    df = df.copy()
    df["sc_rank"] = df["ROAD_RANK"].apply(score_road_rank)
    df["sc_lanes"] = df["LANES"].apply(score_lanes)
    df["sc_spd"] = df["MAX_SPD"].apply(score_max_spd)
    df["sc_type"] = df["ROAD_TYPE"].apply(score_road_type)

    df["weighted"] = (
        df["sc_rank"] * 0.30
        + df["sc_lanes"] * 0.25
        + df["sc_spd"] * 0.25
        + df["sc_type"] * 0.20
    )
    return df


def assign_grade(df):
    """가중합산 점수를 pd.cut으로 1~5 등급으로 분류한다."""
    df = df.copy()
    df["road_grade"] = pd.cut(
        df["weighted"],
        bins=5,
        labels=[1, 2, 3, 4, 5],
        include_lowest=True,
    ).astype(int)
    return df


def build(config):
    """도로 위험도 등급 산출 파이프라인."""
    out_cfg = config["output"]

    # 1. shapefile 로드 + 필터링
    filtered = load_and_filter(config["source"], config["filter"])

    # 2. 링크별 속성 추출
    cols = ["LINK_ID", "ROAD_RANK", "LANES", "MAX_SPD", "ROAD_TYPE"]
    missing = [c for c in cols if c not in filtered.columns]
    if missing:
        print(f"shapefile에 필요한 컬럼이 없습니다: {missing}")
        print(f"사용 가능한 컬럼: {list(filtered.columns)}")
        sys.exit(1)

    links = filtered[cols].copy()
    links = links.drop_duplicates(subset=["LINK_ID"])
    print(f"고유 링크 수: {len(links)}")

    # 3. 점수화 + 가중합산
    links = compute_weighted_score(links)

    # 4. 등급 분류
    links = assign_grade(links)

    print(f"\n링크 등급 분포:\n{links['road_grade'].value_counts().sort_index()}")

    # 5. 기존 세그먼트 CSV 읽어서 s_id ↔ link_id 매핑
    seg_path = config.get("segments_path", out_cfg["path"])
    if not os.path.exists(seg_path):
        print(f"세그먼트 파일을 찾을 수 없습니다: {seg_path}")
        print("config.yaml에 segments_path를 설정하거나 build_segments.py를 먼저 실행하세요.")
        sys.exit(1)

    segments = pd.read_csv(seg_path) if seg_path.endswith(".csv") else pd.read_parquet(seg_path)
    print(f"세그먼트 수: {len(segments)}")

    # 6. link_id 기준 조인
    grade_map = links[["LINK_ID", "road_grade"]].rename(columns={"LINK_ID": "link_id"})
    grade_map["link_id"] = grade_map["link_id"].astype(str)
    segments["link_id"] = segments["link_id"].astype(str)
    result = segments[["s_id", "link_id"]].merge(grade_map, on="link_id", how="left")

    unmatched = result["road_grade"].isna().sum()
    if unmatched > 0:
        print(f"경고: {unmatched}개 세그먼트의 등급을 매핑할 수 없습니다.")
        result["road_grade"] = result["road_grade"].fillna(3).astype(int)
    else:
        result["road_grade"] = result["road_grade"].astype(int)

    # 7. 저장
    grade_path = os.path.join(os.path.dirname(seg_path), "road_grade_863.csv")

    os.makedirs(os.path.dirname(grade_path), exist_ok=True)
    result.to_csv(grade_path, index=False)

    print(f"\n총 세그먼트 수: {len(result)}")
    print(f"등급 분포:\n{result['road_grade'].value_counts().sort_index()}")
    print(f"저장 완료: {grade_path}")


if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    config = load_config(config_path)
    build(config)
