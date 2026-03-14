SELECT
    user_id,
    MIN(event_time)::date AS first_seen_date,
    MAX(event_time)::date AS last_seen_date,
    COUNT(*) AS total_events,
    COUNT(DISTINCT session_id) AS sessions,
    COUNT(DISTINCT DATE(event_time)) AS active_days
FROM {{ ref('stg_raw_events') }}
GROUP BY user_id
