-- Convert datelist records into 
WITH params AS (
    SELECT :today::date AS d
),

starter AS (
    SELECT
        user_id,
        browser_type,
        dates_active @> ARRAY[valid_date::date] AS is_active,
        EXTRACT(DAY FROM d - valid_date) AsS days_since
    FROM user_devices_cumulated t1 CROSS JOIN (
        SELECT GENERATE_SERIES(d - interval '31 days', d, '1 day'::interval) AS valid_date
        FROM params
    ) t2 INNER JOIN params t3 ON TRUE
    WHERE curr_date = d
),

-- Integer bitmask using bit shift: oldest day -> bit 0; today -> bit 31
bits AS (
    SELECT
        user_id,
        browser_type,
        -- SUM(CASE WHEN is_active THEN POW(2, 31 - days_since)::bigint
        --     ELSE 0 END)::BIGINT::BIT(32) AS datelist_int,
        -- in PostgreSQL: integer max = 2³¹−1.
        SUM(CASE WHEN is_active THEN 1::bigint << (31 - days_since)
            ELSE 0 END) AS datelist_int,
        d as curr_date
    FROM starter t1 INNER JOIN params t2
        ON TRUE
    GROUP BY user_id, browser_type, d
)

SELECT * 
FROM bits;