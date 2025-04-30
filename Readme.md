# TfL Real-Time Transport Data Pipeline

**Author:** abdool   
**Tech Stack:** Apache Airflow, Apache Kafka, Snowflake, MinIO, Streamlit, Docker  

---

## Project Overview

This project showcases an end-to-end real-time **data engineering pipeline** built with open data from **Transport for London (TfL)** using **one bus line (line 24)** and **one tube line (victoria)**.

It ingests **live transport data** (arrivals, service disruptions, timetables, stop point metadata), streams arrivals and disruptions data using **Kafka** while **Airflow** is used to orchestrate daily updates on timetable and stop point metadata. Raw and cleaned snapshots are stored in **MinIO**. The curated datasets is loaded into a **Snowflake** warehouse, and insights are presented in an interactive **Streamlit dashboard**.

> This project demonstrates my ability to design, orchestrate, and deliver **scalable**, **real-time** data pipelines across multiple technologies.

---

## Features

- **Real-time data ingestion** from TfL APIs (Arrivals, Timetables, Disruptions, Stop Points).
- **Streaming ingestion** via **Kafka** topics.
- **Workflow orchestration** with **Apache Airflow** DAGs.
- **Raw data lake** storage using **MinIO** (S3-compatible).
- **Data warehouse** modeling and loading into **Snowflake**.
- **Data transformation** to curate clean datasets.
- **Automated view creation** for easy querying.
- **Live Streamlit dashboard** with auto-refreshing metrics and graphs.
- **Fully containerized** using **Docker Compose** for easy local setup.

---

## Tech Stack

| Component        | Technology           |
|------------------|-----------------------|
| Orchestration    | Apache Airflow         |
| Streaming        | Apache Kafka           |
| Storage          | MinIO (for raw JSON snapshots) |
| Data Warehouse   | Snowflake              |
| Dashboard        | Streamlit              |
| Containerization | Docker & Docker Compose |
| Scripting        | Python 3               |

---

## Project Architecture


flowchart TD
```
  A[TfL APIs (Arrivals, Timetables, Disruptions, Stop Points)] --> B[Kafka Producer]
  B --> C[Kafka Topics]
  C --> D[Airflow DAGs]
  D --> E[MinIO (raw and cleaned storage)]
  D --> F[Transformations (Python scripts)]
  F --> G[Snowflake (curated warehouse)]
  G --> H[Streamlit Dashboard]
```
## How to Reproduce
Pre-requisites:
    Docker and Docker Compose installed
    Snowflake account
    TfL Open API account (for API keys)

Clone the repo
```
git clone https://github.com/yourusername/tfl-realtime-pipeline.git
cd tfl-realtime-pipeline
```

Create Snowflake Tables
Run all the queries in snowflake/create_tables.sql and then snowflake/create_views.sql  in your snowflake warehouse 

Create .env file in the project root with:
```
AIRFLOW_UID=50000
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=tfl_data
TFL_APP_ID=your_tfl_app_id
TFL_APP_KEY=your_tfl_app_key
BUCKET_NAME=your_bucket_name
BASE_URL=ttps://api.tfl.gov.uk
S3_ENDPOINT_URL=your_endpoint_url,
AWS_ACCESS_KEY_ID=your_aws_access_key_id,
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
```

Start Kafka, MinIO, and Airflow with Docker Compose
docker-compose up -d

Create airflow connections
Create an s3 connection to conn_id as **minio_conn** and a snowflake connection with conn_id **snowflake_conn** using your credentials.

## Launch Streamlit Dashboard
```
pip install -r requirements.txt
streamlit run dashboard.py
```
The dashboard will open in your browser at http://localhost:8501/








