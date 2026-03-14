from __future__ import annotations

from pathlib import Path

import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_EVENTS_PATH = PROJECT_ROOT / "data/raw/events.csv"
OUTPUT_PRED_PATH = PROJECT_ROOT / "data/processed/churn_predictions.csv"


def _build_training_frame(events: pd.DataFrame) -> pd.DataFrame:
    events["event_time"] = pd.to_datetime(events["event_time"], utc=True)
    user_metrics = (
        events.groupby("user_id")
        .agg(
            total_events=("event_id", "count"),
            sessions=("session_id", "nunique"),
            active_days=("event_time", lambda series: series.dt.date.nunique()),
            last_seen=("event_time", "max"),
        )
        .reset_index()
    )

    threshold = user_metrics["last_seen"].quantile(0.5)
    user_metrics["churn"] = (user_metrics["last_seen"] < threshold).astype(int)
    return user_metrics


def run_churn_prediction(input_path: Path = INPUT_EVENTS_PATH, output_path: Path = OUTPUT_PRED_PATH) -> Path:
    events = pd.read_csv(input_path)
    if events.empty:
        raise ValueError("Input events file is empty")

    data = _build_training_frame(events)
    features = ["total_events", "sessions", "active_days"]
    X = data[features]
    y = data["churn"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42, stratify=y
    )

    model = RandomForestClassifier(n_estimators=200, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    print(classification_report(y_test, y_pred, digits=3))

    data["churn_probability"] = model.predict_proba(X)[:, 1]
    data["predicted_churn"] = (data["churn_probability"] >= 0.5).astype(int)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    data.to_csv(output_path, index=False)
    return output_path


if __name__ == "__main__":
    path = run_churn_prediction()
    print(f"Churn predictions saved to: {path}")
