import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

password = quote_plus('aadi@9055')
engine = create_engine(
    f"postgresql+psycopg2://postgres:{password}@localhost/job_market"
)

df_jobs = pd.read_sql("SELECT * FROM transformed_jobs", engine)
df_skills = pd.read_sql("SELECT * FROM job_skills", engine)

df_jobs.to_csv('data/transformed_jobs.csv', index=False)
df_skills.to_csv('data/job_skills.csv', index=False)

print("Done!")