WITH params AS (
    SELECT :today::date AS td
), 

yesterday AS (
    SELECT *
    FROM host_activity_reduced INNER JOIN params
        ON TRUE
    WHERE curr_date = td::date - INTERVAL '1 DAY'
),

today AS (
    SELECT
        event_time::date AS curr_date,
        host,
        COUNT(1) AS hits,
        COUNT(DISTINCT user_id) AS uniq_visitors
    FROM web_events INNER JOIN params
        ON TRUE
    WHERE DATE_TRUNC('DAY', event_time)::date = td
        AND user_id IS NOT NULL
    GROUP BY DATE_TRUNC('DAY', event_time)::date, host
)

INSERT INTO host_activity_reduced
SELECT
    DATE_TRUNC('month', td)::date AS month,
    COALESCE(t1.host, t2.host) AS host,
    COALESCE((CASE WHEN DATE_TRUNC('month', t1.curr_date) = DATE_TRUNC('month', t2.curr_date) 
        THEN t1.hit_array 
        ELSE ARRAY_FILL(0, ARRAY[(td - DATE_TRUNC('month', td)::date)::int]) END),
        ARRAY_FILL(0, ARRAY[(td - DATE_TRUNC('month', td)::date)::int])) || 
            ARRAY[COALESCE(t2.hits, 0)]
        AS hit_array,
    COALESCE((CASE WHEN DATE_TRUNC('month', t1.unique_visitors) = DATE_TRUNC('month', t2.uniq_visitors) 
        THEN t1.unique_visitors 
        ELSE ARRAY_FILL(0, ARRAY[(td - DATE_TRUNC('month', td)::date)::int]) END),
        ARRAY_FILL(0, ARRAY[(td - DATE_TRUNC('month', td)::date)::int])) || 
            ARRAY[COALESCE(t2.uniq_visitors, 0)] 
        AS unique_visitors,
    COALESCE(t2.curr_date, t1.curr_date + INTERVAL '1 DAY') AS curr_date
FROM yesterday t1 FULL OUTER JOIN today t2
    ON t1.host = t2.host 
;