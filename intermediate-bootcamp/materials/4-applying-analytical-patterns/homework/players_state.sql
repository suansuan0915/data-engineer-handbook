WITH seasons AS (
  SELECT DISTINCT season FROM player_seasons
),
players AS (
  SELECT DISTINCT player_name FROM player_seasons
),
calendar AS (
  SELECT player_name, season
  FROM players 
  CROSS JOIN seasons 
),
player_active AS (
  SELECT player_name, season, 1 AS is_active
  FROM player_seasons 
),
timeline AS (
  SELECT
    c.player_name,
    c.season,
    COALESCE(a.is_active, 0) AS is_active
  FROM calendar c
  LEFT JOIN player_active a
    ON a.player_name = c.player_name
   AND a.season = c.season
),
labeled AS (
  SELECT
    player_name,
    season,
    is_active,
    LAG(is_active) OVER (PARTITION BY player_name ORDER BY season) AS prev_active,
    MIN(CASE WHEN is_active = 1 THEN season END) OVER (PARTITION BY player_name) AS first_active_season
  FROM timeline 
)

SELECT
  player_name,
  season,
  CASE
    WHEN is_active = 1 AND season = first_active_season THEN 'New'
    WHEN is_active = 1 AND COALESCE(prev_active, 0) = 1 THEN 'Continued Playing'
    WHEN is_active = 1 AND COALESCE(prev_active, 0) = 0 AND season != first_active_season THEN 'Returned from Retirement'
    WHEN is_active = 0 AND COALESCE(prev_active, 0) = 1 THEN 'Retired'
    WHEN is_active = 0 AND COALESCE(prev_active, 0) = 0 THEN 'Stayed Retired'
  END AS player_status
FROM labeled
ORDER BY player_name, season
;