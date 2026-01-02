CREATE TABLE IF NOT EXISTS processed_events_aggregated (
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    ip VARCHAR,
    host VARCHAR,
    events_in_session BIGINT
);

CREATE TABLE IF NOT EXISTS processed_session_events_aggregated_tc (
    host VARCHAR,
    avg_events_in_session DECIMAL(10,2)
);
CREATE TABLE IF NOT EXISTS processed_session_events_aggregated_zachtc (
    host VARCHAR,
    avg_events_in_session DECIMAL(10,2)
);
CREATE TABLE IF NOT EXISTS processed_session_events_aggregated_zacht (
    host VARCHAR,
    avg_events_in_session DECIMAL(10,2)
);
CREATE TABLE IF NOT EXISTS processed_session_events_aggregated_lulu (
    host VARCHAR,
    avg_events_in_session DECIMAL(10,2)
);

-- select * from processed_events_aggregated;
-- drop table processed_events_aggregated;
-- drop table processed_session_events_aggregated_lulu;
-- drop table processed_session_events_aggregated_tc;
-- drop table processed_session_events_aggregated_zachtc;
-- drop table processed_session_events_aggregated_zacht;