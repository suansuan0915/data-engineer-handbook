------ Backfill scd query ------
-- one query to populate up-to-date table.

WITH prev_current_info AS (
    SELECT
        actorid,
        actor,
        quality_class,
        LAG(quality_class, 1) OVER(PARTITION BY actorid ORDER BY current_year) AS prev_quality_class,
        is_active,
        LAG(is_active, 1) OVER(PARTITION BY actorid ORDER BY current_year) AS prev_is_active,
        current_year
    FROM actors
    WHERE current_year <= 9999  -- normalizing “open-ended” semantics
),

with_indicators AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        -- using <> can yield NULL ("not sure") when prev values are NULL
        -- better: use "IS DISTINCT FROM" instead.
        CASE WHEN quality_class <> prev_quality_class THEN 1
            WHEN is_active <> prev_is_active THEN 1
            ELSE 0 END AS change_indicator,
        current_year    
    FROM prev_current_info
),

with_streaks AS (
    SELECT
        actorid,
        actor,
        current_year,
        quality_class,
        is_active,
        change_indicator,
        SUM(change_indicator) OVER(PARTITION BY actorid ORDER BY current_year) AS streaks
    FROM with_indicators
    -- ORDER BY 1
),

aggregated AS (
    SELECT
        actorid,
        actor,
        streaks,
        quality_class,
        is_active,
        MIN(current_year) AS start_date,
        MAX(current_year) AS end_date
    FROM with_streaks
    GROUP BY actorid, actor, streaks, quality_class, is_active
    -- order by 1,4
)

INSERT INTO actors_scd_query
SELECT 
    actorid,
    actor,
    quality_class,
    is_active,
    start_date,
    end_date,
    DATE_PART('year', CURRENT_DATE) AS current_year
FROM aggregated
ORDER BY actorid, start_date
;
