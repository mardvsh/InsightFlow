# InsightFlow — Product Analytics & Anomaly Detection Platform

Production-style аналитическая платформа для продуктовой команды:
данные собираются из GitHub API, загружаются в PostgreSQL, трансформируются в dbt,
анализируются SQL-запросами, обрабатываются ML-моделями и показываются в Streamlit UI.

## Скриншоты системы

Dashboard (EN):

![Dashboard RU](docs/screenshots/insight.png)

## Что внутри

- Python ETL: сбор и загрузка данных (`etl/api_collector.py`, `etl/data_loader.py`)
- PostgreSQL как warehouse (`raw_events` + marts через dbt)
- dbt-проект (`staging` + `marts` + тесты)
- SQL-аналитика (DAU, cohort, funnel)
- ML: anomaly detection + churn prediction
- Streamlit-дешборд с переключением RU/EN
- Docker Compose для локального запуска PostgreSQL
- Scheduler для автоматического ежедневного прогона

## Требования

Перед запуском нужно:

- Python 3.11+
- Docker Desktop (Windows/Mac) или Docker Engine (Linux)
- В Docker Desktop статус **Engine running**

Проверка:

```bash
python --version
docker version
docker compose version
```

## Запуск с нуля (если скачали с GitHub)

```bash
git clone https://github.com/mardvsh/InsightFlow.git
cd InsightFlow
python -m venv .venv
```

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

## Быстрый локальный запуск (полный pipeline)

### 1) Поднять PostgreSQL

```bash
docker compose up -d
docker compose ps
```

### 2) Сбор и загрузка данных

```bash
python etl/api_collector.py
python etl/data_loader.py
```

### 3) dbt-трансформации

```bash
cd dbt_project
dbt run --profiles-dir .
dbt test --profiles-dir .
cd ..
```

### 4) ML-слой

```bash
python ml/anomaly_detection.py
python ml/churn_prediction.py
```

### 5) Запуск UI

```bash
streamlit run dashboard/app.py --server.port 8501
```

## Куда заходить в браузере

- Dashboard: `http://localhost:8501`
- PostgreSQL port: `localhost:5432`

## Что есть в UI

В верхней части страницы:

- тумблер `Русский / English` (переключает все подписи интерфейса)

На странице доступны:

- тумблер `English` (ON = English, OFF = Русский)
- индикатор текущего языка `Текущий язык / Current language: EN|RU`
- график DAU и сессий по дням
- таблица дневных метрик
- блок аномалий (счётчик + таблица аномальных дней)
- график распределения churn
- таблица top-20 пользователей по риску churn

## Откуда данные

Источник по умолчанию: GitHub Issues API для активного репозитория.

По умолчанию используется:

- `microsoft/vscode`

Можно переключить источник через `.env`:

```env
GITHUB_REPO=owner/repository
DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/insightflow
```

Файл `.env` загружается автоматически в `etl/api_collector.py`.

Примеры более активных репозиториев:

- `microsoft/vscode`
- `kubernetes/kubernetes`
- `microsoft/TypeScript`

## Как проверить, что всё работает

1. После `etl/api_collector.py` обновляется файл `data/raw/events.csv`.
2. После `etl/data_loader.py` есть вывод `Loaded rows: ...` и обновляется `data/processed/daily_metrics.csv`.
3. `dbt test` завершается `PASS` без `ERROR`.
4. После ML обновляются:
	- `data/processed/daily_metrics_with_anomalies.csv`
	- `data/processed/churn_predictions.csv`
5. `http://localhost:8501` открывается и показывает графики/таблицы.

Быстрая проверка endpoint:

```powershell
Invoke-WebRequest -Uri http://localhost:8501 -UseBasicParsing
```

## Как остановить

Остановить только БД:

```bash
docker compose stop
```

Остановить и удалить контейнеры/сеть:

```bash
docker compose down
```

Полный сброс (включая volume PostgreSQL):

```bash
docker compose down -v
```

## Как запустить заново

После `stop`:

```bash
docker compose start
```

После `down`:

```bash
docker compose up -d
```

## Полезные команды

Статус контейнеров:

```bash
docker compose ps
```

Логи PostgreSQL:

```bash
docker compose logs -f postgres
```

Локальный nightly-like прогон (ручной):

```bash
python etl/api_collector.py
python etl/data_loader.py
python ml/anomaly_detection.py
python ml/churn_prediction.py
```

Автоматический scheduler:

```bash
python automation/scheduler.py
```

## SQL и аналитика

- Product metrics: `analytics/product_metrics.sql`
- Cohort analysis: `analytics/cohort_analysis.sql`
- Funnel analysis: `analytics/funnel_analysis.sql`
- Additional queries: `warehouse/queries.sql`

## Документация

- Архитектура: `docs/architecture.md`
- Метрики: `docs/metrics.md`
- Data Dictionary: `docs/data_dictionary.md`
- Roadmap (7–10 days): `docs/roadmap_7_10_days.md`

## Если не запускается

### Ошибка подключения к PostgreSQL (`connection refused`)

1. Проверь Docker Engine
2. Подними базу: `docker compose up -d`
3. Проверь `docker compose ps`

### dbt не видит профиль

Запускай dbt из папки `dbt_project`:

```bash
cd dbt_project
dbt run --profiles-dir .
dbt test --profiles-dir .
```

Если `dbt` не найден в PATH, используй venv-бинарник:

```powershell
cd dbt_project
..\.venv\Scripts\dbt.exe run --profiles-dir .
..\.venv\Scripts\dbt.exe test --profiles-dir .
```

### UI открылся, но пусто

Сначала прогоните:

```bash
python etl/api_collector.py
python etl/data_loader.py
python ml/anomaly_detection.py
python ml/churn_prediction.py
```

## Архитектура (кратко)

`GitHub API` → `ETL` → `PostgreSQL raw_events` → `dbt staging/marts` → `SQL + ML` → `Streamlit`

---

# English Quick Guide

## What this project is

InsightFlow is a local production-style analytics platform for product teams:

- API ingestion
- warehouse loading
- dbt transformations
- SQL product analytics
- anomaly detection and churn prediction
- interactive dashboard with RU/EN language toggle

## Quick start

```bash
git clone https://github.com/mardvsh/InsightFlow.git
cd InsightFlow
python -m venv .venv
pip install -r requirements.txt
docker compose up -d
python etl/api_collector.py
python etl/data_loader.py
cd dbt_project && dbt run --profiles-dir . && dbt test --profiles-dir . && cd ..
python ml/anomaly_detection.py
python ml/churn_prediction.py
streamlit run dashboard/app.py --server.port 8501
```

## URLs

- Dashboard: `http://localhost:8501`
- PostgreSQL: `localhost:5432`

## Stop and restart

```bash
docker compose stop
docker compose down
docker compose up -d
```
