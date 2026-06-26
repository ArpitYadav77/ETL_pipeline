import os
import sys
import logging
import requests
from pathlib import Path
from dotenv import load_dotenv

# Configure logging to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# AWS Configurations
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")

# Target dataset files
DATASETS = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv"
}

# Hugging Face Repository base URL hosting the raw CSV files
BASE_URL = "https://huggingface.co/datasets/miminmoons/olist-ecommerce-for-delivery-and-review-prediction/resolve/main/data"

def download_file(filename: str, local_path: Path) -> bool:
    """
    Downloads a single CSV file from Hugging Face in chunks.
    """
    url = f"{BASE_URL}/{filename}"
    logger.info(f"Starting download of {filename} from {url}...")
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                if chunk:
                    f.write(chunk)
        logger.info(f"Successfully downloaded {filename} to {local_path} ({local_path.stat().st_size / (1024*1024):.2f} MB)")
        return True
    except Exception as e:
        logger.error(f"Failed to download {filename}: {str(e)}")
        return False

def upload_to_s3(local_path: Path, s3_key: str) -> bool:
    """
    Uploads a file to AWS S3 using boto3.
    """
    if not AWS_ACCESS_KEY_ID or not AWS_S3_BUCKET:
        logger.warning(f"AWS credentials or S3 bucket not configured. Skipping S3 upload for {local_path.name}.")
        return False
    
    try:
        import boto3
        from botocore.exceptions import NoCredentialsError
        
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        logger.info(f"Uploading {local_path.name} to s3://{AWS_S3_BUCKET}/{s3_key}...")
        s3_client.upload_file(str(local_path), AWS_S3_BUCKET, s3_key)
        logger.info(f"Successfully uploaded {local_path.name} to S3.")
        return True
    except ImportError:
        logger.error("boto3 package is not installed. Unable to upload to S3.")
        return False
    except NoCredentialsError:
        logger.error("AWS credentials invalid or not found.")
        return False
    except Exception as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        return False

def run_ingestion():
    """
    Orchestrates the download and upload of Olist dataset files.
    """
    # Create local raw_data caching folder
    raw_dir = Path(__file__).parent / "raw_data"
    raw_dir.mkdir(exist_ok=True, parents=True)
    
    success = True
    for key, filename in DATASETS.items():
        local_path = raw_dir / filename
        s3_key = f"raw/{filename}"
        
        # Download file if it doesn't already exist locally (caching for quick dev/test)
        if not local_path.exists():
            download_ok = download_file(filename, local_path)
            if not download_ok:
                success = False
                continue
        else:
            logger.info(f"File {filename} already exists locally. Using cached version.")
            
        # Try S3 upload if AWS is configured
        upload_to_s3(local_path, s3_key)
        
    if success:
        logger.info("Ingestion completed successfully.")
    else:
        logger.error("Ingestion completed with errors.")
        sys.exit(1)

if __name__ == "__main__":
    run_ingestion()
