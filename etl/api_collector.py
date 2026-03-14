from __future__ import annotations

from pathlib import Path
import hashlib
import os
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_PATH = PROJECT_ROOT / "data/raw/events.csv"
GITHUB_API_URL = "https://api.github.com/repos/{repo}/issues"
DEFAULT_GITHUB_REPO = "microsoft/vscode"

load_dotenv(PROJECT_ROOT / ".env")


def _safe_user_id(login: str) -> int:
    digest = hashlib.md5(login.encode("utf-8")).hexdigest()[:8]
    return int(digest, 16)


def fetch_github_issues(repo: str = DEFAULT_GITHUB_REPO, per_page: int = 100) -> pd.DataFrame:
    url = GITHUB_API_URL.format(repo=repo)
    response = requests.get(
        url,
        params={"state": "all", "per_page": per_page},
        headers={"Accept": "application/vnd.github+json"},
        timeout=30,
    )
    response.raise_for_status()
    payload: list[dict[str, Any]] = response.json()

    rows: list[dict[str, Any]] = []
    for issue in payload:
        if "pull_request" in issue:
            continue

        user = issue.get("user") or {}
        user_login = user.get("login") or "unknown"

        rows.append(
            {
                "event_id": int(issue["id"]),
                "issue_number": int(issue["number"]),
                "user_id": _safe_user_id(user_login),
                "user_login": user_login,
                "session_id": f"issue-{issue['number']}",
                "event_type": "issue_opened" if issue.get("state") == "open" else "issue_closed",
                "feature_used": "issues",
                "event_time": issue.get("created_at"),
                "source": "github_api",
            }
        )

    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame["event_time"] = pd.to_datetime(frame["event_time"], utc=True)
    return frame


def save_raw_events(df: pd.DataFrame, output_path: Path = RAW_DATA_PATH) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return output_path


def run_collection(repo: str | None = None, per_page: int = 100) -> Path:
    selected_repo = repo or os.getenv("GITHUB_REPO", DEFAULT_GITHUB_REPO)
    df = fetch_github_issues(repo=selected_repo, per_page=per_page)
    return save_raw_events(df)


if __name__ == "__main__":
    path = run_collection()
    print(f"Raw events saved to: {path}")
