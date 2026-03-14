CREATE TABLE IF NOT EXISTS raw_events (
    event_id BIGINT PRIMARY KEY,
    issue_number INTEGER,
    user_id BIGINT,
    user_login TEXT,
    session_id TEXT,
    event_type TEXT,
    feature_used TEXT,
    event_time TIMESTAMPTZ,
    source TEXT
);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_time ON raw_events (event_time);
CREATE INDEX IF NOT EXISTS idx_raw_events_user_id ON raw_events (user_id);
