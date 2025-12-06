--- Player scored the most points in one season
SELECT gd.player_id, g.season, SUM(gd.pts::int) AS total_points
FROM game_details gd
    JOIN games g USING (game_id)
GROUP BY gd.player_id, g.season
ORDER BY total_points DESC
LIMIT 1;