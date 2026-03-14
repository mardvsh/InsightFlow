from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DEFAULT_DB_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/insightflow"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_PATH = PROJECT_ROOT / "data/raw/events.csv"
SCHEMA_PATH = PROJECT_ROOT / "warehouse/schema.sql"
DAILY_METRICS_PATH = PROJECT_ROOT / "data/processed/daily_metrics.csv"


def get_engine():
    db_url = os.getenv("DATABASE_URL", DEFAULT_DB_URL)
    return create_engine(db_url)


def apply_schema(engine) -> None:
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(schema_sql))


def load_raw_events(engine, file_path: Path = RAW_DATA_PATH) -> int:
    df = pd.read_csv(file_path)
    if df.empty:
        return 0

    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    df = df.drop_duplicates(subset=["event_id"])

    with engine.begin() as conn:
        df.to_sql("raw_events_staging", con=conn, if_exists="replace", index=False, method="multi", chunksize=1000)
        conn.execute(
            text(
                """
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
                SELECT
                    event_id,
                    issue_number,
                    user_id,
                    user_login,
                    session_id,
                    event_type,
                    feature_used,
                    event_time,
                    source
                FROM raw_events_staging
                ON CONFLICT (event_id) DO NOTHING;
                """
            )
        )
        conn.execute(text("DROP TABLE IF EXISTS raw_events_staging"))

    return len(df)


def export_daily_metrics(engine, output_path: Path = DAILY_METRICS_PATH) -> Path:
    sql = """
    SELECT
        DATE(event_time) AS day,
        COUNT(*) AS events,
        COUNT(DISTINCT user_id) AS dau,
        COUNT(DISTINCT session_id) AS sessions
    FROM raw_events
    GROUP BY 1
    ORDER BY 1;
    """
    df = pd.read_sql(sql, engine)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return output_path


def run_load() -> None:
    engine = get_engine()
    apply_schema(engine)
    loaded_rows = load_raw_events(engine)
    metrics_path = export_daily_metrics(engine)
    print(f"Loaded rows: {loaded_rows}")
    print(f"Daily metrics exported to: {metrics_path}")


if __name__ == "__main__":
    run_load()
