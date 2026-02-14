-- Layer 3: Serving - 인덱스 전략
-- Raw Layer (pothole_segments) 조회 성능 최적화

-- 1. 날짜 범위 조회 (대시보드 시간 필터)
CREATE INDEX IF NOT EXISTS idx_pothole_date
    ON pothole_segments(date DESC);

-- 2. 세그먼트별 조회 (특정 도로 포커싱)
CREATE INDEX IF NOT EXISTS idx_pothole_segment
    ON pothole_segments(s_id);

-- 3. 지리정보 조회 (지도 경계상자 필터)
CREATE INDEX IF NOT EXISTS idx_pothole_geo
    ON pothole_segments(centroid_lon, centroid_lat);

-- 4. 복합 인덱스 (date + s_id) - Materialized View REFRESH 시 활용
CREATE INDEX IF NOT EXISTS idx_pothole_date_segment
    ON pothole_segments(date DESC, s_id);

-- 5. impact_count 범위 필터 (위험도별 필터링)
CREATE INDEX IF NOT EXISTS idx_pothole_impact
    ON pothole_segments(impact_count DESC);