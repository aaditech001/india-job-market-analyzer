import requests
import os
import json
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

TEST_MODE = True# Set to False for production


class JobScraper:

    def __init__(self):
        self.url = "https://jsearch.p.rapidapi.com/search"
        self.roles = ["Data Scientist", "Data Analyst", "Data Engineer"]
        self.all_jobs = []
        self.headers = {
            "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY"),
            "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
        }

    def collect_all_job(self):
        for role in self.roles:
            for page in range(1, 4):  # Scrape first 3 pages for each role
                querystring ={
            "query":f"{role} india",
            "page":page,
            "num_pages": "1",
            "date_posted": "week"
        }
        #-----------------------Fetch the data----------------------
                if TEST_MODE:
                    with open("mock_response.json", "r") as f:
                        response_data = json.load(f)
                    print(f"TEST MODE — {role} page {page}")
                    print(f"Keys in response: {response_data.keys()}") 

                else:
                    try:
                        #real API call
                        response = requests.get(self.url, headers=self.headers, params=querystring)
                        response.raise_for_status()  # Check for HTTP errors
                        response_data = response.json()
                        # save as mock for future testing
                        with open("mock_response.json", "w") as f:
                            json.dump(response_data, f, indent=2)
                        print("Mock data saved!")
                    except requests.RequestException as e:
                        print(f" Request failed: {e}")
                        continue  
                    
                # ----------------------Process the jobs----------------------
                jobs = response_data.get("data", []) 
                print(f"Found {len(jobs)} jobs for {role} on page {page}")
                for job in jobs:
                    structured_data = {
                        "role_searched": role,
                        "job_title": job.get("job_title"),
                        "employer_name": job.get("employer_name"),
                        "job_description": job.get("job_description"),
                        "job_employment_type": job.get("job_employment_type"),
                        "job_city": job.get("job_city"),
                        "job_country": job.get("job_country"),
                        "job_apply_link": job.get("job_apply_link"),
                        "posted_date": job.get("job_posted_at_datetime_utc")
                    }
                    self.all_jobs.append(structured_data)
                    print(f" Job added: {structured_data['job_title']} at {structured_data['employer_name']}")
          
    def save_to_csv(self,filename):

        df = pd.DataFrame(self.all_jobs)
        df.to_csv(filename, index=False)
        print(f"\nTotal jobs collected: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        print("\nFirst 3 rows:")
        print(df.head(3))

    def load_to_postgres(self):
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST"),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD")
            )
        except psycopg2.Error as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return
        cursor = conn.cursor()

        for job in self.all_jobs:
            cursor.execute("""
                INSERT INTO raw_jobs (
                    role_searched, job_title, employer_name,
                    job_description, job_employment_type,
                    job_city, job_country, job_apply_link, posted_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                job["role_searched"],
                job["job_title"],
                job["employer_name"],
                job["job_description"],
                job["job_employment_type"],
                job["job_city"],
                job["job_country"],
                job["job_apply_link"],
                job["posted_date"]
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ {len(self.all_jobs)} jobs loaded into PostgreSQL")

        

if __name__ == "__main__":
    scraper = JobScraper()
    scraper.collect_all_job()
    scraper.save_to_csv("all_jobs.csv")
    scraper.load_to_postgres()