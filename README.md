# 🇮🇳 India Job Market Analyzer

An end-to-end automated data pipeline that scrapes, transforms, and analyzes live tech job postings from India's job market.

## 🏗️ Architecture
```
JSearch API → PostgreSQL → Pandas ETL → Apache Airflow → Streamlit Dashboard
```

## 🛠️ Tech Stack

- **Python** — Scraping, ETL, Analysis
- **PostgreSQL** — Data Storage
- **Apache Airflow** — Pipeline Orchestration
- **Docker** — Containerization
- **Streamlit** — Interactive Dashboard
- **SQL** — Window Functions, CTEs, Analytics

## 📊 Key Findings

- **Python** is the #1 demanded skill (38 jobs)
- **Bengaluru & Gurugram** tied as top hiring cities
- **95% jobs are Onsite** — remote work still rare in India
- **Excel** is Bengaluru's #1 demanded skill — not Python!

  ## 🚀 Live Dashboard
[View Live Dashboard](https://india-job-market-analyzer-gwjzvobaya7gzybjk4nxwj.streamlit.app)

## 📁 Project Structure
```
├── scraper.py              # Extract layer — JSearch API
├── tranformations.ipynb    # Transform layer — Pandas ETL
├── anlaysis.sql            # 12 SQL analytical queries
├── dashboard.py            # Streamlit dashboard
├── airflow/
│   ├── dags/               # Airflow DAG
│   └── docker-compose.yml  # Docker config
└── requirements.txt
```

## 🚀 How to Run

1. Clone the repo
2. Add `.env` file with DB credentials
3. Run `docker-compose up -d` in `/airflow`
4. Run `python -m streamlit run dashboard.py`
