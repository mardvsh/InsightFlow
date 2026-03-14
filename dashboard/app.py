from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAILY_PATH = PROJECT_ROOT / "data/processed/daily_metrics.csv"
ANOMALY_PATH = PROJECT_ROOT / "data/processed/daily_metrics_with_anomalies.csv"
CHURN_PATH = PROJECT_ROOT / "data/processed/churn_predictions.csv"

st.set_page_config(page_title="InsightFlow Dashboard", layout="wide")

I18N = {
    "ru": {
        "lang_toggle": "English / Русский",
        "title": "InsightFlow — Дашборд продуктовой аналитики",
        "subtitle": "ETL + PostgreSQL + dbt + ML: единая витрина метрик, аномалий и churn-прогноза.",
        "core_metrics": "Основные метрики",
        "core_empty": "Пока нет дневных метрик. Запусти ETL и загрузчик.",
        "core_chart_title": "График DAU и сессий по дням",
        "core_chart_caption": "Линейный график: ежедневные активные пользователи (DAU) и количество сессий.",
        "core_table_title": "Таблица дневных метрик (последние 10 дней)",
        "core_table_caption": "Таблица с колонками day, events, dau, sessions.",
        "anomaly_header": "Аномалии",
        "anomaly_empty": "Пока нет файла с аномалиями. Запусти ml/anomaly_detection.py",
        "anomaly_metric": "Найдено аномалий",
        "anomaly_table_title": "Таблица аномальных дней",
        "anomaly_table_caption": "Показывает только дни, где модель отметила аномалию.",
        "churn_header": "Прогноз оттока (Churn)",
        "churn_empty": "Пока нет предсказаний churn. Запусти ml/churn_prediction.py",
        "churn_chart_title": "График распределения churn",
        "churn_chart_caption": "Столбчатый график количества пользователей по классам риска.",
        "churn_table_title": "Таблица top-20 пользователей по риску churn",
        "churn_table_caption": "Чем выше churn_probability, тем выше риск оттока.",
        "risk_axis": "Класс риска",
        "users_count": "Количество пользователей",
        "low_risk": "Низкий риск",
        "high_risk": "Высокий риск",
        "col_day": "День",
        "col_events": "События",
        "col_dau": "DAU",
        "col_sessions": "Сессии",
        "col_anomaly_flag": "Флаг аномалии",
        "col_is_anomaly": "Аномалия",
        "col_user_id": "Пользователь",
        "col_total_events": "Всего событий",
        "col_active_days": "Активных дней",
        "col_last_seen": "Последняя активность",
        "col_churn": "Факт churn",
        "col_churn_probability": "Вероятность churn",
        "col_predicted_churn": "Прогноз churn",
    },
    "en": {
        "lang_toggle": "English / Русский",
        "title": "InsightFlow — Product Analytics Dashboard",
        "subtitle": "ETL + PostgreSQL + dbt + ML in one view: metrics, anomaly alerts, and churn prediction.",
        "core_metrics": "Core Metrics",
        "core_empty": "No daily metrics yet. Run ETL and loader first.",
        "core_chart_title": "DAU and Sessions Trend",
        "core_chart_caption": "Line chart for daily active users (DAU) and sessions by day.",
        "core_table_title": "Daily Metrics Table (last 10 days)",
        "core_table_caption": "Table with day, events, dau, and sessions columns.",
        "anomaly_header": "Anomaly Alerts",
        "anomaly_empty": "No anomaly output yet. Run ml/anomaly_detection.py",
        "anomaly_metric": "Detected anomalies",
        "anomaly_table_title": "Anomalous Days Table",
        "anomaly_table_caption": "Shows only days flagged as anomalies by the model.",
        "churn_header": "Churn Prediction",
        "churn_empty": "No churn predictions yet. Run ml/churn_prediction.py",
        "churn_chart_title": "Churn Distribution Chart",
        "churn_chart_caption": "Bar chart of users split by predicted churn risk class.",
        "churn_table_title": "Top-20 Users by Churn Risk",
        "churn_table_caption": "Higher churn_probability means higher churn risk.",
        "risk_axis": "Risk class",
        "users_count": "Users",
        "low_risk": "Low risk",
        "high_risk": "High risk",
        "col_day": "Day",
        "col_events": "Events",
        "col_dau": "DAU",
        "col_sessions": "Sessions",
        "col_anomaly_flag": "Anomaly flag",
        "col_is_anomaly": "Anomaly",
        "col_user_id": "User",
        "col_total_events": "Total events",
        "col_active_days": "Active days",
        "col_last_seen": "Last seen",
        "col_churn": "Churn (actual)",
        "col_churn_probability": "Churn probability",
        "col_predicted_churn": "Predicted churn",
    },
}

top_left, top_right = st.columns([7, 2])
with top_right:
    english_enabled = st.toggle("English", value=False, help="ON = English, OFF = Русский")
    current_lang = "EN" if english_enabled else "RU"
    st.caption(f"Текущий язык / Current language: **{current_lang}**")

lang = "en" if english_enabled else "ru"
t = I18N[lang]

with top_left:
    st.title(t["title"])
    st.caption(t["subtitle"])


@st.cache_data
def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


daily = load_csv(DAILY_PATH)
anomalies = load_csv(ANOMALY_PATH)
churn = load_csv(CHURN_PATH)

daily_renamed = daily.rename(
    columns={
        "day": t["col_day"],
        "events": t["col_events"],
        "dau": t["col_dau"],
        "sessions": t["col_sessions"],
    }
)

anomalies_renamed = anomalies.rename(
    columns={
        "day": t["col_day"],
        "events": t["col_events"],
        "dau": t["col_dau"],
        "sessions": t["col_sessions"],
        "anomaly_flag": t["col_anomaly_flag"],
        "is_anomaly": t["col_is_anomaly"],
    }
)

churn_renamed = churn.rename(
    columns={
        "user_id": t["col_user_id"],
        "total_events": t["col_total_events"],
        "sessions": t["col_sessions"],
        "active_days": t["col_active_days"],
        "last_seen": t["col_last_seen"],
        "churn": t["col_churn"],
        "churn_probability": t["col_churn_probability"],
        "predicted_churn": t["col_predicted_churn"],
    }
)

left, right = st.columns(2)

with left:
    st.subheader(t["core_metrics"])
    if daily_renamed.empty:
        st.info(t["core_empty"])
    else:
        st.markdown(f"**{t['core_chart_title']}**")
        st.caption(t["core_chart_caption"])
        st.line_chart(daily_renamed.set_index(t["col_day"])[[t["col_dau"], t["col_sessions"]]])

        st.markdown(f"**{t['core_table_title']}**")
        st.caption(t["core_table_caption"])
        st.dataframe(daily_renamed.tail(10), width="stretch")

with right:
    st.subheader(t["anomaly_header"])
    if anomalies_renamed.empty:
        st.info(t["anomaly_empty"])
    else:
        alert_rows = anomalies_renamed[anomalies_renamed[t["col_is_anomaly"]] == 1]
        st.metric(t["anomaly_metric"], int(alert_rows.shape[0]))

        st.markdown(f"**{t['anomaly_table_title']}**")
        st.caption(t["anomaly_table_caption"])
        st.dataframe(alert_rows, width="stretch")

st.subheader(t["churn_header"])
if churn_renamed.empty:
    st.info(t["churn_empty"])
else:
    churn_counts = churn_renamed[t["col_predicted_churn"]].value_counts().sort_index()
    churn_chart_df = churn_counts.rename(index={0: t["low_risk"], 1: t["high_risk"]}).rename_axis(t["risk_axis"]).to_frame(t["users_count"])

    st.markdown(f"**{t['churn_chart_title']}**")
    st.caption(t["churn_chart_caption"])
    st.bar_chart(churn_chart_df)

    st.markdown(f"**{t['churn_table_title']}**")
    st.caption(t["churn_table_caption"])
    st.dataframe(churn_renamed.sort_values(t["col_churn_probability"], ascending=False).head(20), width="stretch")
