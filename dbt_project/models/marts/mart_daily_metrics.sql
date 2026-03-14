SELECT
    DATE(event_time) AS day,
    COUNT(*) AS events,
    COUNT(DISTINCT user_id) AS dau,
    COUNT(DISTINCT session_id) AS sessions,
    COUNT(DISTINCT CASE WHEN event_type = 'issue_closed' THEN user_id END) AS users_closed_issues
FROM {{ ref('stg_raw_events') }}
GROUP BY DATE(event_time)
