from __future__ import annotations

from pathlib import Path

import pandas as pd
from sklearn.ensemble import IsolationForest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = PROJECT_ROOT / "data/processed/daily_metrics.csv"
OUTPUT_PATH = PROJECT_ROOT / "data/processed/daily_metrics_with_anomalies.csv"


def detect_anomalies(input_path: Path = INPUT_PATH, output_path: Path = OUTPUT_PATH) -> Path:
    df = pd.read_csv(input_path)
    if df.empty:
        raise ValueError("Input dataset is empty")

    model = IsolationForest(contamination=0.02, random_state=42)
    df["anomaly_flag"] = model.fit_predict(df[["dau"]])
    df["is_anomaly"] = df["anomaly_flag"].map({1: 0, -1: 1})

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return output_path


if __name__ == "__main__":
    path = detect_anomalies()
    print(f"Anomaly metrics saved to: {path}")
