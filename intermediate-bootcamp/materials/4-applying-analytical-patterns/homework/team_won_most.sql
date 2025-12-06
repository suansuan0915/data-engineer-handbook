--- Team won the most games
WITH team_games AS (
  SELECT
    g.game_id,
    t.team_id,
    CASE
      WHEN t.team_id = g.home_team_id AND g.home_team_wins = 1 THEN 1
      WHEN t.team_id = g.visitor_team_id AND g.home_team_wins = 0 THEN 1
      ELSE 0
    END::int AS won
  FROM games g
  CROSS JOIN LATERAL (VALUES (g.home_team_id), (g.visitor_team_id)) AS t(team_id)
)
SELECT team_id, SUM(won) AS total_wins
FROM team_games
GROUP BY team_id
ORDER BY total_wins DESC
LIMIT 1;