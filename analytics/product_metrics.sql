-- Core product metrics: DAU, events, sessions, actions per user
WITH daily AS (
    SELECT
        DATE(event_time) AS day,
        COUNT(*) AS events,
        COUNT(DISTINCT user_id) AS dau,
        COUNT(DISTINCT session_id) AS sessions
    FROM raw_events
    GROUP BY 1
)
SELECT
    day,
    events,
    dau,
    sessions,
    ROUND(events::numeric / NULLIF(dau, 0), 2) AS events_per_user,
    ROUND(events::numeric / NULLIF(sessions, 0), 2) AS events_per_session
FROM daily
ORDER BY day;
