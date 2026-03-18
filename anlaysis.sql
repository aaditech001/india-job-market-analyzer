SELECT * FROM job_skills
limit 10;   

SELECT * from transformed_jobs
LIMIT 10;

------------------------------
-- INDIAN JOBS MARKET ANALYSIS
-------------------------------

--Query 1: Top 10 most in-demand skills in the Indian job market
select 
    skill ,COUNT(*) as demand_count
FROM job_skills
GROUP BY skill
ORDER BY demand_count DESC
LIMIT 10;  

--Query 2: JOBS BY CITY
SELECT job_city, COUNT(*) as job_count
FROM transformed_jobs
GROUP BY job_city
ORDER BY job_count DESC
LIMIT 10;  

--Query 3: JOBS BY ROLE
SELECT role_searched, COUNT(*) as job_count
FROM transformed_jobs
GROUP BY role_searched
ORDER BY job_count DESC
LIMIT 10;  

--Query 4 :Top hiring companies in the Indian job market
SELECT company_name, COUNT(*) as job_count  
FROM transformed_jobs
GROUP BY company_name
order by job_count desc;

--Query 5: Employment type distribution in the Indian job market
SELECT job_employment_type, COUNT(*) as count
FROM transformed_jobs
GROUP BY job_employment_type
ORDER BY count DESC;

--Query 6: skills by Role 
select tj.role_searched, js.skill ,count(*) as skill_count
FROM job_skills js
join transformed_jobs tj
    on js.job_id = tj.id
where tj.role_searched IN 
    ('Data Scientist','Data Analyst','Data Engineer')
group by tj.role_searched, skill
order by count(*) desc;

--Query 7: city wise role distribution
SELECT job_city, role_searched, COUNT(*) as job_count
from transformed_jobs
GROUP BY job_city, role_searched
order by job_city, job_count desc;  

--Query 8: Top skill per city 
with job_skill_count as (
    select job_city, skill, COUNT(*) as skill_count,
           ROW_NUMBER() 
           OVER (PARTITION BY job_city 
                ORDER BY COUNT(*) DESC) as rn
    from job_skills js
    join transformed_jobs tj-
        on js.job_id = tj.id
    group by job_city, skill
)

select job_city , skill
from job_skill_count
where rn=1
order by job_city;

--Query 9: company wise role distribution
SELECT company_name, role_searched, COUNT(*) as job_count
from transformed_jobs
GROUP BY company_name, role_searched;

--Query 10: Most common job titles in the Indian job market
SELECT job_title, COUNT(*) as job_count
FROM transformed_jobs
GROUP BY job_title
ORDER BY job_count DESC;

--Query 11: Most common skills combination 
SELECT tj.role_searched as role, j1.skill as skill1, j2.skill as skill2, COUNT(*) as combo_count
FROM job_skills j1
JOIN job_skills j2 ON j1.job_id = j2.job_id AND j1.skill < j2.skill
JOIN transformed_jobs tj ON j1.job_id = tj.id
GROUP BY tj.role_searched, j1.skill, j2.skill
ORDER BY combo_count DESC;

--Query 12: Remote vs On-site job ratio
select CASE 
        WHEN LOWER(job_title) LIKE '%remote%' 
          OR LOWER(job_city) LIKE '%remote%' THEN 'Remote'
        ELSE 'Onsite'
    END as work_type,
COUNT(*) as count
from transformed_jobs
GROUP BY work_type
ORDER BY count DESC;