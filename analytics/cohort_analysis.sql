-- Cohort retention by first seen month
WITH first_seen AS (
    SELECT
        user_id,
        DATE_TRUNC('month', MIN(event_time))::date AS cohort_month
    FROM raw_events
    GROUP BY user_id
),
activity AS (
    SELECT
        user_id,
        DATE_TRUNC('month', event_time)::date AS activity_month
    FROM raw_events
    GROUP BY user_id, DATE_TRUNC('month', event_time)
)
SELECT
    f.cohort_month,
    a.activity_month,
    COUNT(DISTINCT a.user_id) AS active_users
FROM first_seen f
JOIN activity a USING (user_id)
GROUP BY 1, 2
ORDER BY 1, 2;
