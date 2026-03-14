-- DAU by day
SELECT
    DATE(event_time) AS day,
    COUNT(DISTINCT user_id) AS dau
FROM raw_events
GROUP BY 1
ORDER BY 1;

-- User activity summary
SELECT
    user_id,
    COUNT(*) AS actions,
    COUNT(DISTINCT session_id) AS sessions
FROM raw_events
GROUP BY 1
ORDER BY actions DESC;

-- Cumulative events by user (window function)
SELECT
    user_id,
    event_time,
    COUNT(*) OVER (PARTITION BY user_id ORDER BY event_time) AS cumulative_events
FROM raw_events
ORDER BY user_id, event_time;

-- Event type distribution
SELECT
    event_type,
    COUNT(*) AS total_events
FROM raw_events
GROUP BY 1
ORDER BY total_events DESC;
