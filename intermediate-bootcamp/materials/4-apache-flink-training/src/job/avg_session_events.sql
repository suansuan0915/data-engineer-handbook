INSERT INTO processed_session_events_aggregated_tc
SELECT 
    'Tech Creator' as host, 
    ROUND(AVG(events_in_session), 2) AS avg_events_in_session
FROM processed_events_aggregated
WHERE host LIKE '%.techcreator.io'
GROUP BY host
;