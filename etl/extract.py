# Part of: E-commerce ETL Pipeline & RAG Support Assistant
"""
Extract module for the E-commerce ETL Pipeline.
Handles reading local raw CSV data, uploading to MinIO S3-compatible storage,
and caching as a parquet file.
"""

import os
from datetime import datetime
from pathlib import Path
import pandas as pd
import boto3
from botocore.client import Config
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def extract_and_upload(local_path: str = "data/data.csv") -> None:
    """
    Reads raw CSV from local_path, uploads it to MinIO, and saves it as parquet.
    
    Args:
        local_path (str): Path to the raw CSV file.
    """
    print(f"[EXTRACT] Starting extract_and_upload process using file: {local_path}...")
    
    # Resolve local path
    local_file_path = Path(local_path)
    if not local_file_path.exists():
        raise FileNotFoundError(f"Source file not found at {local_file_path}")
        
    # Read the CSV file
    print(f"[EXTRACT] Reading CSV from {local_file_path}...")
    df = pd.read_csv(local_file_path, encoding="ISO-8859-1")
    print(f"[EXTRACT] Successfully read CSV. Shape: {df.shape}")
    
    # Load configuration
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    minio_bucket = os.getenv("MINIO_BUCKET", "ecom-raw")
    
    # Ensure endpoint is correctly formatted as URL
    endpoint_url = minio_endpoint if minio_endpoint.startswith("http") else f"http://{minio_endpoint}"
    
    print(f"[EXTRACT] Connecting to MinIO at {endpoint_url}...")
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )
    
    # Create the bucket if it does not exist
    try:
        s3.head_bucket(Bucket=minio_bucket)
        print(f"[EXTRACT] Bucket '{minio_bucket}' already exists.")
    except Exception:
        print(f"[EXTRACT] Bucket '{minio_bucket}' does not exist. Creating bucket...")
        s3.create_bucket(Bucket=minio_bucket)
        print(f"[EXTRACT] Created bucket '{minio_bucket}' successfully.")
        
    # Define S3 destination key
    today_str = datetime.now().strftime("%Y/%m/%d")
    s3_key = f"raw/{today_str}/orders.csv"
    
    # Upload the raw CSV
    print(f"[EXTRACT] Uploading CSV to MinIO at bucket={minio_bucket}, key={s3_key}...")
    s3.upload_file(str(local_file_path), minio_bucket, s3_key)
    print(f"[EXTRACT] Successfully uploaded CSV to MinIO path: s3://{minio_bucket}/{s3_key}")
    
    # Save the dataframe as parquet to /tmp/raw_orders.parquet
    parquet_path = Path("/tmp/raw_orders.parquet")
    print(f"[EXTRACT] Saving dataframe as parquet to {parquet_path}...")
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False)
    
    print(f"[EXTRACT] Completed extract_and_upload process successfully. Cached to {parquet_path}")

if __name__ == "__main__":
    extract_and_upload()
