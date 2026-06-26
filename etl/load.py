# Part of: E-commerce ETL Pipeline & RAG Support Assistant
"""
Load module for the E-commerce ETL Pipeline.
Handles connecting to PostgreSQL and writing transformed datasets into tables.
"""

import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def load_to_postgres(input_dir: str = "/tmp") -> None:
    """
    Reads the four cached parquet files from input_dir, connects to PostgreSQL
    using DB_URL from the environment, adds a created_at timestamp column,
    and loads each dataset into its corresponding table.
    
    Args:
        input_dir (str): Directory containing the parquet files to load.
    """
    print("[LOAD] Starting load_to_postgres process...")
    
    # Paths for input files
    in_dir = Path(input_dir)
    file_mappings = {
        "orders": in_dir / "raw_orders.parquet",
        "monthly_revenue": in_dir / "monthly_revenue.parquet",
        "top_customers": in_dir / "top_customers.parquet",
        "top_products": in_dir / "top_products.parquet"
    }
    
    # Load database URL
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL environment variable is not set in .env")
        
    print(f"[LOAD] Creating database engine for URL: {db_url.split('@')[-1] if '@' in db_url else db_url}")
    engine = create_engine(db_url)
    
    # Load each file and save to Postgres
    with engine.connect() as conn:
        print("[LOAD] Database connection established successfully.")
        
        for table_name, file_path in file_mappings.items():
            print(f"[LOAD] Processing table '{table_name}' from {file_path}...")
            
            if not file_path.exists():
                raise FileNotFoundError(f"Required parquet file for load not found: {file_path}")
                
            # Read parquet
            df = pd.read_parquet(file_path)
            
            # Add created_at timestamp column
            current_time = pd.Timestamp.now()
            df["created_at"] = current_time
            print(f"[LOAD] Added created_at timestamp: {current_time} to {table_name}")
            
            # Load to SQL using context manager connection
            print(f"[LOAD] Writing {len(df)} rows to table '{table_name}'...")
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists="replace",
                index=False
            )
            # For SQLAlchemy 2.0, commit might be needed on connection context
            # We can execute commit if the connection has commit method or just call commit on transaction if needed.
            # Pandas to_sql usually commits, but let's execute a commit to be absolutely safe.
            try:
                conn.commit()
            except Exception:
                # Some database wrappers/drivers might not require manual commit
                pass
                
            print(f"[LOAD] Table '{table_name}' loaded successfully. Row count confirmed: {len(df)}.")
            
    print("[LOAD] Completed load_to_postgres process successfully.")

if __name__ == "__main__":
    load_to_postgres()
