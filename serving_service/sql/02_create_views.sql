-- 1. 기존 materialized VIEW가 있다면 삭제
DROP MATERIALIZED VIEW IF EXISTS mvw_dashboard_heatmap CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mvw_dashboard_weekly_stats CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mvw_dashboard_repair_priority CASCADE;

-- ==============================================================================
-- [1] 위험 히트맵 구체화 뷰 (mvw_dashboard_heatmap)
-- 당일 세그먼트별 통행량 대비 충격 횟수를 계산하여 지도 시각화에 사용합니다.
-- 🌟 수정: 전체 데이터가 아닌 '가장 최신 날짜'의 데이터만 가져옵니다.
-- ==============================================================================
CREATE MATERIALIZED VIEW mvw_dashboard_heatmap AS
SELECT 
    ps.date,                                 
    ps.s_id,                                 
    sa.road_name,                            
    sa.district,                             
    ps.centroid_lon,                         
    ps.centroid_lat,                         
    ps.total_count,                          
    ps.impact_count,                         
    CASE 
        WHEN ps.total_count > 0 THEN ROUND((ps.impact_count::NUMERIC / ps.total_count) * 100, 2)
        ELSE 0 
    END AS risk_rate
FROM 
    pothole_segments ps
LEFT JOIN 
    segment_address sa ON ps.s_id = sa.s_id
WHERE 
    ps.date = (SELECT MAX(date) FROM pothole_segments); -- 🌟 최신 날짜 필터링

CREATE UNIQUE INDEX idx_mvw_heatmap_sid_date ON mvw_dashboard_heatmap (s_id, date);


-- ==============================================================================
-- [2] 일주일 통계 구체화 뷰 (mvw_dashboard_weekly_stats)
-- 최근 7일 동안의 요일별 통행량 및 도로 상태 비교를 위한 데이터를 제공합니다.
-- 🌟 수정: '가장 최신 날짜'를 기준으로 과거 7일치 데이터만 가져옵니다.
-- ==============================================================================
CREATE MATERIALIZED VIEW mvw_dashboard_weekly_stats AS
SELECT 
    ps.date,                                 
    EXTRACT(ISODOW FROM ps.date) AS dow_num, 
    TO_CHAR(ps.date, 'Day') AS day_of_week,  
    ps.s_id,
    sa.road_name,
    ps.total_count,                          
    ps.impact_count                          
FROM 
    pothole_segments ps
LEFT JOIN 
    segment_address sa ON ps.s_id = sa.s_id
WHERE 
    ps.date >= (SELECT MAX(date) - INTERVAL '6 days' FROM pothole_segments) 
    AND ps.date <= (SELECT MAX(date) FROM pothole_segments); -- 🌟 최신 기준 7일 필터링

CREATE UNIQUE INDEX idx_mvw_weekly_sid_date ON mvw_dashboard_weekly_stats (s_id, date);


-- ==============================================================================
-- [3] 보수 우선순위 구체화 뷰 (mvw_dashboard_repair_priority)
-- 🌟 수정: '가장 최신 날짜' 하루의 충격량과 시민 민원을 가중 평가하여 순위를 매깁니다.
-- ==============================================================================
CREATE MATERIALIZED VIEW mvw_dashboard_repair_priority AS
-- CTE 1: 가장 최신 날짜 하루치 센서 데이터만 필터링하여 집계
WITH segment_stats AS (
    SELECT 
        MAX(date) AS base_date,              -- 🌟 프론트엔드에서 기준일 표기를 위해 추가
        s_id,
        SUM(impact_count) AS total_impacts,  
        SUM(total_count) AS total_traffic    
    FROM pothole_segments
    WHERE date = (SELECT MAX(date) FROM pothole_segments) -- 🌟 최신 날짜 필터링
    GROUP BY s_id
),
-- CTE 2: 접수된 공공 포트홀 민원 건수를 세그먼트별로 사전 집계
complaint_stats AS (
    SELECT 
        nearest_s_id AS s_id,
        COUNT(id) AS complaint_count         
    FROM pothole_complaints
    GROUP BY nearest_s_id
)
-- 메인 쿼리
SELECT 
    ss.base_date AS date,                    -- 🌟 기준일(최신 날짜) 출력
    ss.s_id,
    sa.road_name,
    sa.district,
    sa.centroid_lat,
    sa.centroid_lon,
    ss.total_traffic,
    ss.total_impacts,
    COALESCE(cs.complaint_count, 0) AS complaint_count,
    (ss.total_impacts * 1.0) + (COALESCE(cs.complaint_count, 0) * 50.0) AS priority_score,
    RANK() OVER (
        ORDER BY ((ss.total_impacts * 1.0) + (COALESCE(cs.complaint_count, 0) * 50.0)) DESC
    ) AS priority_rank
FROM 
    segment_stats ss
LEFT JOIN 
    complaint_stats cs ON ss.s_id = cs.s_id
LEFT JOIN 
    segment_address sa ON ss.s_id = sa.s_id;

CREATE UNIQUE INDEX idx_mvw_priority_sid ON mvw_dashboard_repair_priority (s_id);