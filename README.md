# ETL Data Pipeline Project

## Overview
This project showcases an end-to-end ETL (Extract, Transform, Load) data pipeline developed using various technologies and frameworks. It demonstrates proficiency in data engineering concepts and tools, making it suitable for showcasing skills in data engineering roles.

## Project Details
### Technologies Used
- Apache Airflow
- Python
- Soda
- Astro
- Google Cloud BigQuery
- Google Cloud Storage
- dbt SQL
- Metabase
- Docker

### Pipeline Workflow
1. **Data Acquisition:**
   - Downloaded two CSV datasets from online resources.

2. **Airflow DAG Creation:**
   - Created an Apache Airflow Directed Acyclic Graph (DAG) using the Astro framework.

3. **Data Ingestion:**
   - Ingested the raw datasets into a Google Cloud Storage bucket using Apache Airflow.

4. **Database Creation:**
   - Created a new database and tables based on the CSV files from the Google Cloud Storage bucket in Google BigQuery.

5. **Data Quality Checks (Raw):**
   - Checked the quality of the raw data using the Soda framework.

6. **Data Transformation (Initial):**
   - Transformed the raw data, creating four separate tables using dbt SQL for further analysis.

7. **Data Quality Checks (Transformed):**
   - Checked the quality of the transformed data using the Soda framework.

8. **Data Transformation (Final):**
   - Transformed the data again to create new tables using dbt SQL, enabling simple SQL aggregations to generate reports.

9. **Data Quality Checks (Report):**
   - Ensured the quality of the report data using the Soda framework.

10. **Dashboard Creation:**
    - Integrated Metabase into a Docker container to create interactive dashboards of the report data.

### Automation
- The entire pipeline is automated using Apache Airflow, ensuring seamless execution and minimal manual intervention.

## Usage
To replicate or adapt this data pipeline, follow these steps:
1. Clone the repository.
2. Set up the required environment with Docker.
3. Configure Apache Airflow with the provided DAG file.
4. Update connection configurations for Google Cloud services and Metabase as needed.
5. Run the DAG to execute the ETL pipeline.

## Contributors
- Jodeci Paris
