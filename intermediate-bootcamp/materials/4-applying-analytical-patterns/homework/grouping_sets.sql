WITH player_pts AS (
    SELECT
        t2.game_id,
        player_id,
        team_id,
        season,
        pts
    FROM game_details t1 INNER JOIN games t2
        ON t1.game_id = t2.game_id 
),
team_won AS (
    SELECT
        t1.game_id,
        player_id,
        t2.team_id,
        season,
        CASE 
            WHEN t2.team_id = home_team_id AND home_team_wins = 1 THEN 1
            WHEN t2.team_id = visitor_team_id AND home_team_wins = 0 THEN 1
            ELSE 0
        END AS is_team_won
    FROM player_pts t1 CROSS JOIN LATERAL (
        VALUES (home_team_id), (visitor_team_id)
    ) t2(team_id)
),
base AS (
    SELECT
        player_id,
        team_id,
        season,
        pts,
        0 AS is_team_won
    FROM player_pts 
    UNION ALL
    SELECT
        NULL::int AS player_id,
        team_id,
        NULL::int AS season,
        0 AS pts,
        is_team_won
    FROM team_won 
)

SELECT
    CASE 
        WHEN GROUPING(player_id) = 0 AND GROUPING(team_id) = 0 THEN 'player_team'
        WHEN GROUPING(player_id) = 0 AND GROUPING(season) = 0 THEN 'player_season'
        WHEN GROUPING(team_id) = 0 THEN 'team'
    END AS aggregation_level,
    player_id,
    team_id,
    season,
    SUM(pts) AS total_pts,
    SUM(is_team_won) AS total_won
FROM base
GROUP BY GROUPING SETS (
    (player_id, team_id),
    (player_id, season),
    (team_id)
)
ORDER BY aggregation_level, SUM(pts) DESC NULLS LAST, SUM(is_team_won) DESC NULLS LAST
;