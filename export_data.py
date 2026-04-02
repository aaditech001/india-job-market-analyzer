import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os

load_dotenv()
password = quote_plus(os.getenv("DB_PASSWORD"))
user     = os.getenv("DB_USER")
host     = os.getenv("DB_HOST")
db_name  = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}/{db_name}"
)

df_jobs = pd.read_sql("SELECT * FROM transformed_jobs", engine)
df_skills = pd.read_sql("SELECT * FROM job_skills", engine)

df_jobs.to_csv('data/transformed_jobs.csv', index=False)
df_skills.to_csv('data/job_skills.csv', index=False)

print("Done!")