-- Carry forward historical datelists (which is yesterday's records in the table), 
-- then append today's new records in web_events.
WITH params AS (
    SELECT :today::date AS d
),

yesterday AS (
    SELECT *
    FROM user_devices_cumulated INNER JOIN params
        ON TRUE
    WHERE curr_date = d - INTERVAL '1 DAY'
),

today AS (
    SELECT DISTINCT
        user_id,
        browser_type,
        DATE_TRUNC('day', event_time)::date AS today_date
    FROM web_events t1 INNER JOIN devices t2
        ON t1.device_id = t2.device_id
    WHERE DATE_TRUNC('day', event_time)::date = d
        AND user_id IS NOT NULL
)

INSERT INTO user_devices_cumulated
SELECT
    COALESCE(t1.user_id, t2.user_id::text) AS user_id,
    COALESCE(t1.browser_type, t2.browser_type) AS browser_type,
    COALESCE(dates_active, ARRAY[]::DATE[]) || COALESCE(
        ARRAY[today_date], ARRAY[]::DATE[]
    ) AS dates_active,  --device_activity_datelist
    COALESCE(today_date, t1.curr_date + INTERVAL '1 DAY') AS curr_date
FROM yesterday t1 FULL OUTER JOIN today t2
    ON t1.user_id = t2.user_id::text AND t1.browser_type = t2.browser_type
;