from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
from airflow.models import Variable

os.environ['RAPIDAPI_KEY'] = Variable.get('RAPIDAPI_KEY',default_var='')
os.environ['DB_PASSWORD'] = Variable.get('DB_PASSWORD',default_var='')
os.environ['DB_HOST'] = Variable.get('DB_HOST',default_var='')
os.environ['DB_NAME'] = Variable.get('DB_NAME',default_var='')
os.environ['DB_USER'] = Variable.get('DB_USER',default_var='')

# Project path add karo
sys.path.insert(0, '/opt/airflow/dags')

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def scrape_jobs():
    import sys
    sys.path.insert(0, '/opt/airflow/dags')

    # Override TEST_MODE to False
    import scraper as scraper_module
    scraper_module.TEST_MODE = False
    
    from scraper import JobScraper
    
    s = JobScraper()
    s.collect_all_job()
    s.save_to_csv('/opt/airflow/dags/all_jobs.csv')
    s.load_to_postgres()
    
    print("Scraping complete!")
    return "Scrape success"

def transform_jobs():
    import pandas as pd
    from sqlalchemy import create_engine
    from urllib.parse import quote_plus
    import os
    import re
    
    print("Starting transformation...")
    
    
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_NAME = os.getenv('DB_NAME')

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
    
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_NAME = os.getenv('DB_NAME')

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