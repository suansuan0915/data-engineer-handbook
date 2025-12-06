--- Player who scored the most points playing for one team:
SELECT player_id, team_id, SUM(pts::int) AS total_points
FROM game_details
GROUP BY player_id, team_id
ORDER BY total_points DESC
LIMIT 1