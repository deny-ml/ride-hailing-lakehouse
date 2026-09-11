# Ride-Hailing & Weather Data Lakehouse 

An end-to-end Data Engineering pipeline built to analyze the correlation between geospatial weather conditions and ride-hailing delivery delays. This project applies Medallion Architecture (Bronze, Silver, Gold) to process simulated logistics data and live API weather data, providing actionable insights for transportation operational monitoring and surge pricing strategies.

## Technology Stack
*   **Orchestration:** Apache Airflow (Dockerized)
*   **Data Lake Storage:** Azure Blob Storage
*   **Processing Engine:** Databricks (Serverless PySpark & Pandas)
*   **Serving Layer:** PostgreSQL
*   **Languages:** Python, SQL

## Architecture & Pipeline Flow
1.  **Ingestion (Bronze):** Airflow orchestrates hourly extraction of external Open-Meteo API data and internal logistics mock data, landing raw JSON files directly into Azure Blob Storage.
2.  **Staging (Driver-Node Bypass):** Implemented a custom staging solution using the `azure-storage-blob` Python SDK to bypass Databricks Serverless network isolation, pulling data securely into the local Databricks `/Workspace` for processing.
3.  **Transformation (Silver & Gold):** PySpark processes the raw data by handling missing values, standardizing timestamps, and executing cross-joins to correlate regional weather conditions with logistics delay times.
4.  **Serialization Workaround:** Engineered a workaround for the Databricks Spark Connect `PlanMetrics` JSON serialization bug by utilizing `.collect()` and native Pandas memory conversion to safely write the transformed data as Parquet (Silver Layer) and CSV (Gold Layer).
5.  **Serving:** Aggregated business metrics are pushed back to Azure, pulled by Airflow, and loaded into a local PostgreSQL relational database using SQLAlchemy for downstream BI consumption.

## How to Run Locally

**1. Azure Storage Setup**
* Create an Azure Storage Account via the Azure Portal.
* Navigate to **Data storage > Containers** and create three separate private containers: `bronze`, `silver`, and `gold`.
* Go to **Security + networking > Access keys** and copy your Connection String.

**2. Clone and Environment Setup**
* Clone this repository to your local machine.
* Create a `.env` file in the root directory to map your local user and install dependencies. Configure your environment variables to include:
  * `AIRFLOW_UID` (e.g., `AIRFLOW_UID=50000`)
  * `_PIP_ADDITIONAL_REQUIREMENTS=azure-storage-blob pandas requests sqlalchemy psycopg2-binary apache-airflow-providers-databricks`
  * `AIRFLOW__CORE__FERNET_KEY` (for encrypting Airflow connections)

**3. Databricks Notebook & Job Configuration**
* Open your Databricks Workspace.
* Upload the `ride_hailing_transformation.py` file (located in the `notebooks/` folder of this repository) into your personal Databricks Workspace directory.
* Navigate to the **Workflows** menu, create a new **Job**, and point the task path to the notebook you just uploaded. 
* Set the compute engine to your Existing Interactive Compute (not Serverless). 
* Copy the generated **Job ID** from the Job details panel.

**4. Update Code Placeholders (Identity Mapping)**
Before executing the pipeline, replace the dummy placeholders in the code with your actual credentials and identifiers:
* **In `dags/ride_hailing_pipeline.py`:** Replace `JOB_ID` with the Databricks Job ID you generated. If your operator configuration still uses absolute workspace paths, ensure your `EMAIL` or Workspace Path is updated to match your Databricks login email.
* **In `notebooks/ride_hailing_transformation.py`:** Replace `CONNECTION_STRING_AZURE` with your actual Azure Blob Storage connection string.

**5. Docker Execution & Airflow Connections**
* Run `docker-compose up -d` in your terminal to spin up the Airflow and PostgreSQL containers.
* Access the Airflow UI at `http://localhost:8080`.
* Go to **Admin > Variables** and securely store your Azure connection string with the key `azure_conn_str`.
* Go to **Admin > Connections**, create a new connection named `databricks_conn`, and input your Databricks Host URL and Personal Access Token (PAT).
* Unpause and trigger the `ride_hailing_lakehouse` DAG to start the pipeline.

## Final Output (Serving Layer)
The transformed and aggregated data is successfully loaded into a local PostgreSQL database, creating a clean, analytics-ready table for BI consumption.

<img width="505" height="191" alt="Screenshot 2026-09-10 013720" src="https://github.com/user-attachments/assets/2ac39b63-c450-4b95-aeec-d9a03e7b7462" />
