-- Layer 3: Serving - Materialized View (Mart Layer) 생성
-- Superset이 직접 사용할 집계 및 조인 뷰

-- 1. mv_pothole_context: 포트홀 기본 컨텍스트
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_pothole_context AS
SELECT
    p.s_id,
    p.centroid_lon,
    p.centroid_lat,
    p.date,
    p.impact_count,
    CASE
        WHEN p.impact_count > 5 THEN 'critical'
        WHEN p.impact_count > 3 THEN 'warning'
        ELSE 'normal'
    END AS risk_level
FROM pothole_segments p
ORDER BY p.date DESC, p.s_id;

-- mv_pothole_context 인덱스 (REFRESH CONCURRENTLY 필수)
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_context
    ON mv_pothole_context(s_id, date);

-- 2. mv_daily_summary: 일별 요약 통계
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_summary AS
SELECT
    p.date,
    COUNT(DISTINCT p.s_id) AS affected_segments,
    SUM(p.impact_count) AS total_impacts,
    COUNT(CASE WHEN p.impact_count > 5 THEN 1 END) AS critical_segments,
    COUNT(CASE WHEN p.impact_count > 3 AND p.impact_count <= 5 THEN 1 END) AS warning_segments,
    MIN(p.impact_count) AS min_impacts,
    MAX(p.impact_count) AS max_impacts,
    ROUND(AVG(p.impact_count)::NUMERIC, 2) AS avg_impacts
FROM pothole_segments p
GROUP BY p.date
ORDER BY p.date DESC;

-- mv_daily_summary 인덱스
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_summary
    ON mv_daily_summary(date);

-- 3. mv_repair_priority: 보수 우선순위 (최근 30일 기준)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_repair_priority AS
SELECT
    p.s_id,
    p.centroid_lon,
    p.centroid_lat,
    SUM(p.impact_count) AS total_impacts,
    ROUND(AVG(p.impact_count)::NUMERIC, 2) AS avg_daily_impacts,
    COUNT(DISTINCT p.date) AS detected_days,
    MIN(p.date) AS first_detected,
    MAX(p.date) AS last_detected,
    ROUND(
        SUM(p.impact_count) *
        COUNT(DISTINCT p.date) *
        (1 + LN(GREATEST(SUM(p.impact_count), 1)))::NUMERIC,
        2
    ) AS priority_score
FROM pothole_segments p
WHERE p.date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY p.s_id, p.centroid_lon, p.centroid_lat
ORDER BY priority_score DESC;

-- mv_repair_priority 인덱스
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_repair_priority
    ON mv_repair_priority(s_id);

-- 4. mv_segment_trend: 세그먼트 시계열 추이
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_segment_trend AS
SELECT
    p.s_id,
    p.centroid_lon,
    p.centroid_lat,
    p.date,
    p.impact_count,
    ROUND(
        AVG(p.impact_count) OVER (
            PARTITION BY p.s_id
            ORDER BY p.date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC,
        2
    ) AS rolling_7d_avg,
    (p.impact_count - LAG(p.impact_count, 7) OVER (
        PARTITION BY p.s_id
        ORDER BY p.date
    )) AS week_over_week_change
FROM pothole_segments p
ORDER BY p.s_id, p.date DESC;

-- mv_segment_trend 인덱스
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_segment_trend
    ON mv_segment_trend(s_id, date);