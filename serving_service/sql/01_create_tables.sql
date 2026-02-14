-- Layer 3: Serving - Raw Layer 테이블 생성
-- PostgreSQL 데이터 마트용 원본 테이블 정의

-- pothole_segments 테이블 (핵심 테이블)
CREATE TABLE IF NOT EXISTS pothole_segments (
    s_id           VARCHAR(50) NOT NULL,
    centroid_lon   DOUBLE PRECISION NOT NULL,
    centroid_lat   DOUBLE PRECISION NOT NULL,
    date           DATE NOT NULL,
    impact_count   INTEGER NOT NULL,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (s_id, date)
) PARTITION BY RANGE (date);

-- 초기 파티션 (2026-02)
CREATE TABLE IF NOT EXISTS pothole_segments_2026_02
    PARTITION OF pothole_segments
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

-- 파티션 자동 추가를 위한 주석
-- 매월 1일: CREATE TABLE pothole_segments_2026_03
--     PARTITION OF pothole_segments
--     FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');