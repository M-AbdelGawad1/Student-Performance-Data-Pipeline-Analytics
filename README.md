# 🎓 Student Performance Data Pipeline & Analytics

## 📌 Project Overview
This project focuses on building an **end-to-end data pipeline** and **data warehouse architecture** to analyze student performance.  
The goal is to collect, clean, model, and visualize educational data in order to provide insights that can help improve academic outcomes.

---

## 🚀 Objectives
- Design and implement a **Data Warehouse (DWH)** using **Star Schema** modeling.  
- Apply the **Medallion Architecture (Bronze, Silver, Gold layers)** for data processing.  
- Automate workflows using **Airflow DAGs** with **dbt** transformations.  
- Deliver interactive dashboards in **Power BI** for performance monitoring and decision-making.  

---

## 🏗️ Architecture & Workflow
### 🔹 Data Ingestion
- Source: CSV datasets (student performance and demographics).  
- Loaded into **Snowflake** (Bronze Layer).  

### 🔹 Data Transformation
- **Silver Layer**: Standardization, cleaning (handling missing values, fixing gender column, etc.).  
- **Gold Layer**: Star Schema modeling with **Fact & Dimension tables**.  
- Business logic applied using **dbt models**.  

### 🔹 Orchestration
- **Apache Airflow** used to schedule and orchestrate dbt runs.  
- Automated data refresh workflows.  

### 🔹 Analytics & Visualization
- Final transformed data loaded into **Power BI**.  
- Dashboards created for:  
  - Student academic performance trends  
  - Demographics vs performance  
  - Attendance & study environment impact  

---

## 🛠️ Tech Stack
- **Database & DWH**: Snowflake  
- **Data Transformation**: dbt  
- **Orchestration**: Apache Airflow  
- **Modeling**: Star Schema + Medallion Architecture  
- **Visualization**: Power BI  
- **Languages**: SQL, Python  

---

## 📊 Results & Insights
- Identified key factors influencing student performance.  
- Built **automated dashboards** for ongoing tracking.  
- Provided actionable insights to improve academic outcomes.  

---

## 📂 Project Structure
