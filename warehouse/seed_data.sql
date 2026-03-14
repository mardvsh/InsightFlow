INSERT INTO raw_events (
    event_id,
    issue_number,
    user_id,
    user_login,
    session_id,
    event_type,
    feature_used,
    event_time,
    source
)
VALUES
    (10000001, 1, 101, 'demo_user_1', 'issue-1', 'issue_opened', 'issues', NOW() - INTERVAL '2 day', 'seed'),
    (10000002, 2, 102, 'demo_user_2', 'issue-2', 'issue_opened', 'issues', NOW() - INTERVAL '1 day', 'seed'),
    (10000003, 3, 101, 'demo_user_1', 'issue-3', 'issue_closed', 'issues', NOW(), 'seed')
ON CONFLICT (event_id) DO NOTHING;
