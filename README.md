# Part of: E-commerce ETL Pipeline & RAG Support Assistant

# E-commerce ETL Pipeline & RAG Support Assistant

This end-to-end repository implements a production-grade Data Engineering pipeline and an AI Retrieval-Augmented Generation (RAG) agent. It extracts, validates, transforms, and loads e-commerce transaction data, stores aggregates in PostgreSQL, indexes the company metrics and return policy in ChromaDB, and serves insights via a FastAPI REST interface.

---

## System Architecture

```
 +------------------+
 |  data/data.csv   |  (Raw E-commerce CSV Dataset)
 +--------+---------+
          |
          v [etl/extract.py]
 +------------------+
 |    MinIO S3      |  (Raw CSV Upload, Local Object Storage)
 +--------+---------+
          |
          v [etl/validate.py] & [etl/transform.py]
 +------------------+
 |  Parquet Cache   |  (/tmp/ raw_orders, monthly_revenue, top_customers, top_products)
 +--------+---------+
          |
          v [etl/load.py]
 +------------------+
 | PostgreSQL DB    |  (Structured Tables: orders, monthly_revenue, top_customers, top_products)
 +----+--------+----+
      |        |
      |        v [rag/generate_docs.py] -> [rag/embed.py]
      |   +----+-------------+
      |   | Chroma Vector DB |  (Persisted Vector Store - text-embedding-3-small)
      |   +----+-------------+
      |        |
      v        v [rag/chain.py] (LangChain RetrievalQA + GPT-3.5 Turbo)
 +----+--------+-----+
 |    FastAPI App    |  (api/main.py - REST Endpoints: /ask and /metrics)
 +-------------------+
```

---

## Prerequisites

Before starting, ensure you have the following installed on your machine:
- **Python 3.10+**
- **Docker & Docker Compose**
- **OpenAI API Key** (to run embeddings and LLM queries)
- **curl** or postman (for endpoint verification)

---

## Environment Variables

Copy `.env.example` to `.env` and fill out your variables:

| Variable | Description | Default / Example |
|---|---|---|
| `MINIO_ENDPOINT` | Local MinIO S3 API Endpoint | `localhost:9000` |
| `MINIO_ACCESS_KEY` | Access key for MinIO S3 Console | `minioadmin` |
| `MINIO_SECRET_KEY` | Secret key for MinIO S3 Console | `minioadmin123` |
| `MINIO_BUCKET` | Target raw S3 bucket | `ecom-raw` |
| `DB_URL` | SQLAlchemy PostgreSQL Connection String | `postgresql://ecom_user:ecom_pass@localhost:5432/ecom_db` |
| `OPENAI_API_KEY` | OpenAI Secret API Key | `sk-your-key-here` |
| `AIRFLOW_HOME` | Local Airflow Installation Path | `~/airflow` |

---

## Step-by-Step Setup Instructions

### 1. Set Up Environment & Dependencies
Create and activate a python virtual environment, then install dependencies:
```bash
python -m venv venv

# Windows (Command Prompt)
venv\Scripts\activate
# Windows (PowerShell)
.\venv\Scripts\activate
# Linux/macOS
source venv/bin/activate

pip install -r requirements.txt
```

### 2. Start Services (Docker)
Launch local instances of MinIO and PostgreSQL using docker-compose:
```bash
docker-compose up -d
```
Verify that:
- MinIO console is live at [http://localhost:9001](http://localhost:9001) (User: `minioadmin` | Pass: `minioadmin123`)
- PostgreSQL is listening on port `5432`

### 3. Generate Mock Data (Optional)
If you want to run the pipeline using mock data, the project is pre-configured with a generator. (Already generated at `data/data.csv` for initial testing).

### 4. Setup and Orchestrate with Airflow
Initialize the Airflow database and start orchestrator services:
```bash
# Initialize DB
airflow db init

# Create admin user
airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@local.com
```
Update `airflow.cfg` in your `$AIRFLOW_HOME` directory to point to your project's `dags/` folder as `dags_folder`.

Start services in separate terminal windows:
```bash
# Terminal 1
airflow webserver --port 8080

# Terminal 2
airflow scheduler
```
Open [http://localhost:8080](http://localhost:8080), log in, enable `ecom_etl_pipeline`, and trigger it.

### 5. Index Data to Vector Database (ChromaDB)
After the ETL pipeline completes and loads tables to PostgreSQL, run the embedding script once:
```bash
python rag/embed.py
```
This reads Postgres metrics, formats them to natural text, reads the return policy, builds embeddings, and persists them into `./chroma_store`.

### 6. Run the FastAPI Application
Start the FastAPI server using Uvicorn:
```bash
uvicorn api.main:app --reload --port 8000
```
API Documentation will be interactive at [http://localhost:8000/docs](http://localhost:8000/docs).

---

## Endpoint Verification Examples

Here are `curl` commands to test the five FastAPI endpoints.

### 1. Health Check
```bash
curl -X GET http://localhost:8000/health
```

### 2. Query RAG Assistant (/ask)
```bash
curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "What is the return window for items?"}'
```

### 3. Retrieve Monthly Revenue Metrics
```bash
curl -X GET http://localhost:8000/metrics/revenue
```

### 4. Retrieve Top 20 Products
```bash
curl -X GET http://localhost:8000/metrics/top-products
```

### 5. Retrieve Top 20 Customers
```bash
curl -X GET http://localhost:8000/metrics/top-customers
```
