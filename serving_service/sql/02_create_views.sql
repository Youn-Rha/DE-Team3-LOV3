-- 1. 기존 materialized VIEW가 있다면 삭제
DROP MATERIALIZED VIEW IF EXISTS mvw_dashboard_heatmap CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mvw_dashboard_weekly_stats CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mvw_dashboard_repair_priority CASCADE;

-- ==============================================================================
-- [1] 위험 히트맵 구체화 뷰 (mvw_dashboard_heatmap)
-- 당일 세그먼트별 통행량 대비 충격 횟수를 계산하여 지도 시각화에 사용합니다.
-- ==============================================================================
CREATE MATERIALIZED VIEW mvw_dashboard_heatmap AS
SELECT 
    ps.date,                                 -- 기준 일자
    ps.s_id,                                 -- 세그먼트 ID
    sa.road_name,                            -- 도로명
    sa.district,                             -- 행정구역
    ps.centroid_lon,                         -- 지도 시각화용 경도
    ps.centroid_lat,                         -- 지도 시각화용 위도
    ps.total_count,                          -- 일일 총 통행량
    ps.impact_count,                         -- 일일 총 충격 횟수
    
    -- [위험도 산출 로직] 
    -- 통행량이 0보다 클 때만 (충격수/통행량)*100 연산을 수행하여 에러 방지
    CASE 
        WHEN ps.total_count > 0 THEN ROUND((ps.impact_count::NUMERIC / ps.total_count) * 100, 2)
        ELSE 0 
    END AS risk_rate
FROM 
    pothole_segments ps
LEFT JOIN 
    segment_address sa ON ps.s_id = sa.s_id;

-- 무중단 갱신(CONCURRENTLY)을 위해 반드시 필요한 고유 인덱스 (PK 역할)
CREATE UNIQUE INDEX idx_mvw_heatmap_sid_date ON mvw_dashboard_heatmap (s_id, date);


-- ==============================================================================
-- [2] 일주일 통계 구체화 뷰 (mvw_dashboard_weekly_stats)
-- 최근 7일 동안의 요일별 통행량 및 도로 상태 비교를 위한 데이터를 제공합니다.
-- ==============================================================================
CREATE MATERIALIZED VIEW mvw_dashboard_weekly_stats AS
SELECT 
    ps.date,                                 -- 기준 일자
    EXTRACT(ISODOW FROM ps.date) AS dow_num, -- 1(일) ~ 7(일): 프론트엔드 정렬 기준용
    TO_CHAR(ps.date, 'Day') AS day_of_week,  -- 'Monday' 등 문자열 요일: 화면 표시용
    ps.s_id,
    sa.road_name,
    ps.total_count,                          -- 해당 요일의 통행량
    ps.impact_count                          -- 해당 요일의 충격 횟수
FROM 
    pothole_segments ps
LEFT JOIN 
    segment_address sa ON ps.s_id = sa.s_id;

-- 무중단 갱신용 고유 인덱스
CREATE UNIQUE INDEX idx_mvw_weekly_sid_date ON mvw_dashboard_weekly_stats (s_id, date);


-- ==============================================================================
-- [3] 보수 우선순위 구체화 뷰 (mvw_dashboard_repair_priority)
-- 최근 30일 누적 데이터 기반으로 충격량과 시민 민원을 가중 평가하여 순위를 매깁니다.
-- ==============================================================================

CREATE MATERIALIZED VIEW mvw_dashboard_repair_priority AS
-- CTE 1: 센서 데이터(충격, 통행량)를 세그먼트별로 사전 집계
WITH segment_stats AS (
    SELECT 
        s_id,
        SUM(impact_count) AS total_impacts,  
        SUM(total_count) AS total_traffic    
    FROM pothole_segments
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
-- 메인 쿼리: 사전에 집계된 두 테이블(stats, complaint)을 도로 정보(address)와 결합
SELECT 
    ss.s_id,
    sa.road_name,
    sa.district,
    sa.centroid_lat,
    sa.centroid_lon,
    ss.total_traffic,
    ss.total_impacts,
    
    -- 민원이 없는 곳(NULL)은 0건으로 처리
    COALESCE(cs.complaint_count, 0) AS complaint_count,
    
    -- [점수 산정 로직] 충격 1건 = 1점, 민원 1건 = 50점(가중치)으로 합산
    (ss.total_impacts * 1.0) + (COALESCE(cs.complaint_count, 0) * 50.0) AS priority_score,
    
    -- 계산된 점수(priority_score)를 기준으로 내림차순 정렬하여 순위(1, 2, 3...) 부여
    RANK() OVER (
        ORDER BY ((ss.total_impacts * 1.0) + (COALESCE(cs.complaint_count, 0) * 50.0)) DESC
    ) AS priority_rank
FROM 
    segment_stats ss
LEFT JOIN 
    complaint_stats cs ON ss.s_id = cs.s_id
LEFT JOIN 
    segment_address sa ON ss.s_id = sa.s_id;

-- 무중단 갱신용 고유 인덱스 (이 뷰는 기준 일자(date)가 없으므로 s_id만으로 식별)
CREATE UNIQUE INDEX idx_mvw_priority_sid ON mvw_dashboard_repair_priority (s_id);

