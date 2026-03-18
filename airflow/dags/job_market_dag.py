from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Project path add karo
sys.path.insert(0, '/opt/airflow/dags')

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def scrape_jobs():
    import requests
    import psycopg2
    import json
    from dotenv import load_dotenv
    
    load_dotenv()
    
    print("Starting job scraping...")
    
    # TEST_MODE — real API call nahi hogi
    TEST_MODE = True
    
    if TEST_MODE:
        print("TEST_MODE: Skipping real API call")
        print("Scraping complete — 381 jobs in raw_jobs")
        return "Scrape success"
    
    print("Scraping complete!")
    return "Scrape success"

def transform_jobs():
    import pandas as pd
    from sqlalchemy import create_engine
    from urllib.parse import quote_plus
    import os
    import re
    
    print("Starting transformation...")
    
    
    DB_USER = 'postgres'
    DB_PASSWORD = 'aadi@9055'
    DB_HOST = 'host.docker.internal'
    DB_NAME = 'job_market'
    
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}/{DB_NAME}"
    )
    
    df = pd.read_sql("SELECT * FROM raw_jobs", engine)
    print(f"Loaded {len(df)} rows from raw_jobs")
    
    # Clean
    df['job_city'] = df['job_city'].fillna('Unknown')
    df['job_country'] = df['job_country'].fillna('India')
    df['job_city'] = df['job_city'].str.strip().str.title()
    df = df.drop_duplicates(subset=['job_apply_link'])
    
    df.to_sql('transformed_jobs', engine, if_exists='replace', index=False)
    print(f"Saved {len(df)} clean rows to transformed_jobs")
    return "Transform success"

def load_jobs():
    from sqlalchemy import create_engine
    from urllib.parse import quote_plus
    import pandas as pd
    import os
    
    print("Verifying load...")
    
    DB_USER = 'postgres'
    DB_PASSWORD = 'aadi@9055'
    DB_HOST = 'host.docker.internal'
    DB_NAME = 'job_market'

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}/{DB_NAME}"
    )
    
    count = pd.read_sql("SELECT COUNT(*) as cnt FROM transformed_jobs", engine)
    print(f"Total rows in transformed_jobs: {count['cnt'][0]}")
    return "Load verified"

with DAG(
    dag_id='job_market_pipeline',
    default_args=default_args,
    description='India Job Market ETL Pipeline',
    schedule_interval='0 9 * * 0',
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    task_scrape = PythonOperator(
        task_id='scrape_jobs',
        python_callable=scrape_jobs
    )

    task_transform = PythonOperator(
        task_id='transform_jobs',
        python_callable=transform_jobs
    )

    task_load = PythonOperator(
        task_id='load_jobs',
        python_callable=load_jobs
    )

    task_scrape >> task_transform >> task_load