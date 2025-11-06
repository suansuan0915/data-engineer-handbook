WITH last_year_table AS (
    SELECT *
    FROM actors --the new table
    WHERE current_year = 1971
),

this_year_table AS (
    SELECT *
    FROM actor_films
    WHERE year = 1972
),

recent_year_rating_films AS (
    SELECT 
        actorid,
        1971 AS recent_year,
        ARRAY_AGG(ROW(
            film,
            votes,
            rating,
            filmid
        )::films) AS new_films,
        avg(rating) AS avg_rating
    FROM actor_films
    WHERE year = 1972
    GROUP BY actorid, recent_year
)

-- INSERT INTO actors
SELECT
    COALESCE(t1.actorid, t2.actorid) AS actorid,
    COALESCE(t1.actor, t2.actor) AS actor,
    COALESCE(t1.films, ARRAY[]::films[]) || 
        COALESCE(t3.new_films, ARRAY[]::films[]) AS films,
    CASE WHEN t2.year IS NULL 
        THEN t1.quality_class
        ELSE (CASE WHEN avg_rating > 8 THEN 'star'
            WHEN avg_rating > 7 THEN 'good'
            WHEN avg_rating > 6 THEN 'average'
            ELSE 'bad' END)::quality_class
        END AS quality_class,
    t2.year IS NOT NULL AS is_active,
    1972 AS current_year
FROM last_year_table t1 FULL OUTER JOIN this_year_table t2
    ON t1.actorid = t2.actorid 
    LEFT JOIN recent_year_rating_films t3
    ON t2.actorid = t3.actorid AND recent_year = t2.year
;
