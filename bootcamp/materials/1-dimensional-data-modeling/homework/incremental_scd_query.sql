------ Incremental scd query ------
-- one query to populate up-to-date table.

WITH historical_records AS (
    SELECT *
    FROM actors_scd_query
    WHERE end_date < 1970
        AND current_year = 1970
),

last_year_table AS (
    SELECT *
    FROM actors_scd_query
    WHERE end_date = 1970
        AND current_year = 1970
),

this_year_table AS (
    SELECT *
    FROM actors
    WHERE current_year = 1971
),

new_records AS (
    SELECT
        t1.actorid,
        t1.actor,
        t1.quality_class,
        t1.is_active,
        t1.current_year AS start_date,
        t1.current_year AS end_date,
        t1.current_year AS current_year 
    FROM this_year_table t1 LEFT JOIN last_year_table t2
        ON t1.actorid = t2.actorid
    WHERE t2.actorid IS NULL
),

unchanged_records AS (
    SELECT
        t1.actorid,
        t1.actor,
        t1.quality_class,
        t1.is_active,
        t2.start_date AS start_date,
        t1.current_year AS end_date,
        t1.current_year AS current_year 
    FROM this_year_table t1 INNER JOIN last_year_table t2
        ON t1.actorid = t2.actorid
    WHERE t1.quality_class = t2.quality_class
        AND t1.is_active = t2.is_active
),

changed_records AS (
    SELECT
        t1.actorid,
        t1.actor,
        UNNEST(ARRAY[
            ROW(t2.quality_class, t2.is_active, t2.start_date, t2.end_date)::scd_type,
            ROW(t1.quality_class, t1.is_active, t1.current_year, t1.current_year)::scd_type
        ]) AS records,
        t1.current_year AS current_year
    FROM this_year_table t1 INNER JOIN last_year_table t2
        ON t1.actorid = t2.actorid
    WHERE t1.quality_class <> t2.quality_class
            OR t1.is_active <> t2.is_active
),

unnested_changed_records AS (
    SELECT
        actorid,
        actor,
        (records::scd_type).quality_class AS quality_class,
        (records::scd_type).is_active AS is_active,
        (records::scd_type).start_date AS start_date,
        (records::scd_type).end_date AS end_date,
        current_year 
    FROM changed_records
)

SELECT *
FROM (
    SELECT * FROM historical_records
    UNION ALL
    SELECT * FROM new_records
    UNION ALL
    SELECT * FROM unchanged_records
    UNION ALL
    SELECT * FROM unnested_changed_records
) t
;
