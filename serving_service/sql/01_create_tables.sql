-- Layer 3: Serving - Raw Layer 테이블 생성
-- PostgreSQL 데이터 마트용 원본 테이블 정의

-- pothole_segments 테이블 (Fact)
CREATE TABLE IF NOT EXISTS pothole_segments (
    s_id           VARCHAR(50) NOT NULL,
    centroid_lon   DOUBLE PRECISION NOT NULL,
    centroid_lat   DOUBLE PRECISION NOT NULL,
    date           DATE NOT NULL,
    impact_count   INTEGER NOT NULL,
    total_count    INTEGER NOT NULL,
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

-- segment_address 테이블 (Dimension - SCD Type 1)
CREATE TABLE IF NOT EXISTS segment_address (
    s_id           VARCHAR(50) PRIMARY KEY,
    road_name      VARCHAR(200),
    district       VARCHAR(100),
    centroid_lon   DOUBLE PRECISION,
    centroid_lat   DOUBLE PRECISION,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- pothole_complaints 테이블 (공공데이터 포트홀 민원)
CREATE TABLE IF NOT EXISTS pothole_complaints (
    id             SERIAL PRIMARY KEY,
    create_dt      DATE NOT NULL,
    event_lat      DOUBLE PRECISION NOT NULL,
    event_lon      DOUBLE PRECISION NOT NULL,
    nearest_s_id   VARCHAR(50),
    distance_m     DOUBLE PRECISION,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (create_dt, event_lat, event_lon)
);
