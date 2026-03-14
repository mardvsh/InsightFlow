from __future__ import annotations

import subprocess
import time

import schedule


def run_command(command: str) -> None:
    print(f"Running: {command}")
    subprocess.run(command, shell=True, check=True)


def daily_pipeline() -> None:
    run_command("python etl/api_collector.py")
    run_command("python etl/data_loader.py")
    run_command("python ml/anomaly_detection.py")
    run_command("python ml/churn_prediction.py")


schedule.every().day.at("08:00").do(daily_pipeline)

if __name__ == "__main__":
    print("Scheduler started. Waiting for next run...")
    while True:
        schedule.run_pending()
        time.sleep(30)
