-- Simplified funnel: issue_opened -> issue_closed
WITH steps AS (
    SELECT
        user_id,
        MAX(CASE WHEN event_type = 'issue_opened' THEN 1 ELSE 0 END) AS opened,
        MAX(CASE WHEN event_type = 'issue_closed' THEN 1 ELSE 0 END) AS closed
    FROM raw_events
    GROUP BY user_id
)
SELECT
    SUM(opened) AS users_opened,
    SUM(closed) AS users_closed,
    ROUND(SUM(closed)::numeric / NULLIF(SUM(opened), 0), 4) AS close_conversion
FROM steps;
