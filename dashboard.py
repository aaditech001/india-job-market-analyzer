import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Page config
st.set_page_config(
    page_title="India Job Market Analyzer",
    page_icon="📊",
    layout="wide"
)

# Load data from CSV
@st.cache_data
def load_data():
    df_jobs = pd.read_csv('data/transformed_jobs.csv')
    df_skills = pd.read_csv('data/job_skills.csv')
    return df_jobs, df_skills

df_jobs, df_skills = load_data()

# Sidebar
st.sidebar.title("📊 Navigation")
page = st.sidebar.radio("Go to", [
    "🏠 Overview",
    "🛠️ Skills Analysis", 
    "🏙️ City & Role Analysis",
    "💼 Remote vs Onsite"
])

# ─── Page 1: Overview ────────────────────────────
if page == "🏠 Overview":
    st.title("🇮🇳 India Job Market Analyzer")
    st.markdown("**Analyzing 64 live job postings across Data roles**")
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Jobs", len(df_jobs))
    col2.metric("Unique Skills", df_skills['skill'].nunique())
    col3.metric("Cities Covered", df_jobs['job_city'].nunique())
    col4.metric("Roles Analyzed", df_jobs['role_searched'].nunique())
    
    st.divider()
    
    # Raw data table
    st.subheader("📋 Job Listings")
    st.dataframe(df_jobs[['job_title', 'company_name', 
                           'job_city', 'role_searched', 
                           'job_employment_type']], 
                use_container_width=True)

elif page == "🛠️ Skills Analysis":
    st.title("🛠️ Skills Analysis")
    st.markdown("**Top skills demanded across all job postings**")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📊 Top 10 Skills in demand")
        top_skills = df_skills['skill'].value_counts().head(10).reset_index()
        top_skills.columns = ['Skill', 'Count']
        st.bar_chart(top_skills.set_index('Skill')['Count'])

    
    with col2:
        st.subheader("📈 Skill Distribution by Role")
        skill_role_counts = df_skills.groupby(['role_searched', 'skill']).size().reset_index(name='count')
        selected_role = st.selectbox("Select Role", skill_role_counts['role_searched'].unique())
        filtered_data = skill_role_counts[skill_role_counts['role_searched'] == selected_role]
        st.bar_chart(filtered_data.set_index('skill')['count'])

elif page == "🏙️ City & Role Analysis":
    st.title("🏙️ City & Role Analysis")
    st.markdown("**Analyzing job distribution across cities and roles**")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📊 Job Count by City")
        city_counts = df_jobs['job_city'].value_counts().reset_index()
        city_counts.columns = ['City', 'Count']
        st.bar_chart(city_counts.set_index('City')['Count'])

    with col2:
        st.subheader("📈 Job Count by Role")
        role_counts = df_jobs['role_searched'].value_counts().reset_index()
        role_counts.columns = ['Role', 'Count']
        st.bar_chart(role_counts.set_index('Role')['Count'])

elif page == "💼 Remote vs Onsite":
    st.title("💼 Remote vs Onsite Reality")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Work Type Distribution")
        work_data = pd.DataFrame({
            'Type': ['Onsite', 'Remote'],
            'Count': [61, 3]
        })
        st.bar_chart(work_data.set_index('Type'))
    
    with col2:
        st.subheader("Key Insight")
        st.info("95% of Data jobs in India are Onsite in 2026")
        st.metric("Onsite Jobs", 61)
        st.metric("Remote Jobs", 3)