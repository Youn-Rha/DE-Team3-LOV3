-- Layer 3: Serving - View 생성
-- 기존 Materialized View 제거 후 일반 View 3개 생성

-- 기존 MV 제거
DROP MATERIALIZED VIEW IF EXISTS mv_pothole_context CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_daily_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_repair_priority CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_segment_trend CASCADE;

-- 1. repair_priority: 보수 우선순위 (최근 7일 기준)
--    impact_ratio = impact_count / total_count 포함
CREATE OR REPLACE VIEW repair_priority AS
SELECT
    p.s_id,
    p.centroid_lon,
    p.centroid_lat,
    SUM(p.impact_count) AS total_impacts,
    SUM(p.total_count) AS total_points,
    ROUND(
        (SUM(p.impact_count)::NUMERIC / NULLIF(SUM(p.total_count), 0)),
        4
    ) AS impact_ratio,
    COUNT(DISTINCT p.date) AS detected_days,
    MIN(p.date) AS first_detected,
    MAX(p.date) AS last_detected
FROM pothole_segments p
WHERE p.date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY p.s_id, p.centroid_lon, p.centroid_lat
ORDER BY total_impacts DESC;

-- 2. worsening_alert: 악화 구간 경보
--    rolling 7일 평균 대비 당일 150% 초과 구간
CREATE OR REPLACE VIEW worsening_alert AS
WITH daily AS (
    SELECT
        s_id,
        centroid_lon,
        centroid_lat,
        date,
        impact_count,
        AVG(impact_count) OVER (
            PARTITION BY s_id
            ORDER BY date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS rolling_7d_avg
    FROM pothole_segments
)
SELECT
    d.s_id,
    d.centroid_lon,
    d.centroid_lat,
    d.date,
    d.impact_count,
    ROUND(d.rolling_7d_avg::NUMERIC, 2) AS rolling_7d_avg,
    ROUND(
        (d.impact_count::NUMERIC / NULLIF(d.rolling_7d_avg, 0)) * 100,
        1
    ) AS pct_of_avg
FROM daily d
WHERE d.rolling_7d_avg > 0
  AND d.impact_count > d.rolling_7d_avg * 1.5
  AND d.date = (SELECT MAX(date) FROM pothole_segments)
ORDER BY pct_of_avg DESC;

-- 3. daily_kpi: 전일 대비 탐지 건수/구간 수 변화
CREATE OR REPLACE VIEW daily_kpi AS
WITH daily_stats AS (
    SELECT
        date,
        SUM(impact_count) AS total_impacts,
        COUNT(DISTINCT s_id) AS active_segments
    FROM pothole_segments
    GROUP BY date
),
latest_two AS (
    SELECT
        date,
        total_impacts,
        active_segments,
        LAG(total_impacts) OVER (ORDER BY date) AS prev_impacts,
        LAG(active_segments) OVER (ORDER BY date) AS prev_segments
    FROM daily_stats
)
SELECT
    date,
    total_impacts,
    active_segments,
    total_impacts - COALESCE(prev_impacts, 0) AS impact_change,
    active_segments - COALESCE(prev_segments, 0) AS segment_change
FROM latest_two
ORDER BY date DESC
LIMIT 1;

-- 4. complaint_hotspot: 민원 핫스팟 vs 센서 실측 비교
CREATE OR REPLACE VIEW complaint_hotspot AS
SELECT
    c.nearest_s_id AS s_id,
    sa.road_name,
    sa.district,
    COUNT(*) AS complaint_count,
    rp.total_impacts,
    rp.impact_ratio
FROM pothole_complaints c
LEFT JOIN segment_address sa ON c.nearest_s_id = sa.s_id
LEFT JOIN repair_priority rp ON c.nearest_s_id = rp.s_id
WHERE c.nearest_s_id IS NOT NULL
GROUP BY c.nearest_s_id, sa.road_name, sa.district, rp.total_impacts, rp.impact_ratio
ORDER BY complaint_count DESC;
