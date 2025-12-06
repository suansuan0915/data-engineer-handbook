--- the most # of games in a row LeBron James scored over 10 points:
WITH james_records_flagged AS (
    SELECT
        t1.game_id,
        game_date_est,
        pts,
        CASE WHEN pts > 10 THEN 1 ELSE 0 END AS is_over_10
    FROM games t1 INNER JOIN game_details t2
        ON t1.game_id = t2.game_id
    WHERE player_name = 'LeBron James'
),
runs AS (
  SELECT
    game_id,
    game_date_est,
    pts,
    is_over_10,
    CASE
        WHEN is_over_10 = 1
            AND LAG(is_over_10) OVER (ORDER BY game_date_est, game_id) = 1
        THEN 0 
        WHEN is_over_10 = 1
            AND (LAG(is_over_10) OVER (ORDER BY game_date_est, game_id) IS NULL 
                OR LAG(is_over_10) OVER (ORDER BY game_date_est, game_id)  = 0)
        THEN 1
        ELSE 0 
    END AS start_of_run
  FROM james_records_flagged
),
grouped AS (
    SELECT
        game_id,
        game_date_est,
        pts,
        is_over_10,
        SUM(start_of_run) OVER(ORDER BY game_date_est, game_id) AS run_id
    FROM runs
    WHERE is_over_10 = 1
)

SELECT MAX(run_len) AS longest_over10_streak
FROM (
  SELECT run_id, COUNT(*) AS run_len
  FROM grouped
  GROUP BY run_id
) t
;