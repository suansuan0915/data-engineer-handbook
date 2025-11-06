WITH params AS (
    SELECT :today::date AS d
),

yesterday AS (
    SELECT *
    FROM hosts_cumulated t1 INNER JOIN params t2
        ON TRUE
    WHERE curr_date = d::date - INTERVAL '1 DAY'
),

today AS (
    SELECT DISTINCT
        host,
        url,
        DATE_TRUNC('day', event_time)::date AS curr_date
    FROM web_events t1 INNER JOIN params t2
        ON TRUE
    WHERE DATE_TRUNC('day', event_time)::date = d
)

INSERT INTO hosts_cumulated
SELECT
    COALESCE(t1.host, t2.host) AS host,
    COALESCE(t1.url, t2.url) AS url,
    COALESCE(t1.host_activity_datelist, ARRAY[]::DATE[]) || COALESCE(
        ARRAY[t2.curr_date], ARRAY[]::DATE[]
    ) AS host_activity_datelist,
    COALESCE(t2.curr_date, t1.curr_date + interval '1 day') AS curr_date
FROM yesterday t1 FULL OUTER JOIN today t2
    ON t1.host = t2.host AND t1.url = t2.url
;