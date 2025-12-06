--- the most games a team has won in a 90 game stretch:
WITH team_games AS (
  SELECT
    g.game_id,
    g.game_date_est,
    t.team_id,
    CASE
      WHEN t.team_id = g.home_team_id AND g.home_team_wins = 1 THEN 1
      WHEN t.team_id = g.visitor_team_id AND g.home_team_wins = 0 THEN 1
      ELSE 0
    END::int AS won
  FROM games g
  CROSS JOIN LATERAL (VALUES (g.home_team_id), (g.visitor_team_id)) AS t(team_id)
),
rolling AS (
  SELECT
    team_id,
    game_id,
    game_date_est,
    SUM(won) OVER (
      PARTITION BY team_id
      ORDER BY game_date_est, game_id
      ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS wins_last_90
  FROM team_games
)

SELECT team_id, MAX(wins_last_90) AS max_wins_in_90_games
FROM rolling
GROUP BY team_id
ORDER BY max_wins_in_90_games DESC
LIMIT 1;
