# Part of: E-commerce ETL Pipeline & RAG Support Assistant
"""
Validate module for the E-commerce ETL Pipeline.
Executes data quality checks and handles filtering of invalid rows.
"""

import sys
from pathlib import Path
import pandas as pd

def validate(input_path: str = "/tmp/raw_orders.parquet", output_path: str = "/tmp/validated_orders.parquet") -> None:
    """
    Reads a raw parquet file, performs several data quality validation checks,
    filters invalid rows, and saves the cleaned dataset to output_path.
    
    Args:
        input_path (str): Path to the raw parquet file.
        output_path (str): Path to save the validated parquet file.
    """
    print(f"[VALIDATE] Starting validation process on {input_path}...")
    
    in_path = Path(input_path)
    out_path = Path(output_path)
    
    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found at {in_path}")
        
    print(f"[VALIDATE] Reading parquet from {in_path}...")
    df = pd.read_parquet(in_path)
    initial_rows = len(df)
    print(f"[VALIDATE] Loaded dataset with {initial_rows} rows.")
    
    # 1. Dataset size check
    if initial_rows < 10000:
        raise ValueError(f"Dataset size check failed: expected at least 10,000 rows, found {initial_rows}")
        
    # 2. Quantity column existence check
    if "Quantity" not in df.columns:
        raise ValueError("Validation failed: 'Quantity' column is missing from dataset.")
        
    # 3. CustomerID non-null rate check
    # customer_id can be missing in some datasets, check if CustomerID column exists
    if "CustomerID" not in df.columns:
        raise ValueError("Validation failed: 'CustomerID' column is missing from dataset.")
    
    non_null_rate = df["CustomerID"].notna().mean()
    print(f"[VALIDATE] CustomerID non-null rate: {non_null_rate:.2%}")
    if non_null_rate < 0.80:
        raise ValueError(f"Validation failed: CustomerID non-null rate ({non_null_rate:.2%}) is below the required 80%.")
        
    # 4. UnitPrice never negative check
    if "UnitPrice" not in df.columns:
        raise ValueError("Validation failed: 'UnitPrice' column is missing from dataset.")
    
    if (df["UnitPrice"] < 0).any():
        negative_count = (df["UnitPrice"] < 0).sum()
        raise ValueError(f"Validation failed: Found {negative_count} negative values in UnitPrice column.")
        
    # 5. InvoiceDate parseable as datetime check
    if "InvoiceDate" not in df.columns:
        raise ValueError("Validation failed: 'InvoiceDate' column is missing from dataset.")
        
    try:
        # Check if parseable (we do not assign here, just check)
        pd.to_datetime(df["InvoiceDate"])
        print("[VALIDATE] InvoiceDate is parseable as datetime.")
    except Exception as e:
        raise ValueError(f"Validation failed: InvoiceDate is not parseable as datetime. Error: {e}")
        
    # Data cleaning
    print("[VALIDATE] Performing data cleaning and filtering...")
    
    # Drop rows where CustomerID is null
    df_clean = df.dropna(subset=["CustomerID"])
    
    # Drop rows where Quantity <= 0
    df_clean = df_clean[df_clean["Quantity"] > 0]
    
    # Drop rows where UnitPrice <= 0
    df_clean = df_clean[df_clean["UnitPrice"] > 0]
    
    final_rows = len(df_clean)
    print(f"[VALIDATE] Row count before cleaning: {initial_rows}, after cleaning: {final_rows}")
    print(f"[VALIDATE] Dropped {initial_rows - final_rows} invalid rows ({((initial_rows - final_rows)/initial_rows):.2%} of data).")
    
    # Save to validated path
    print(f"[VALIDATE] Saving validated data as parquet to {out_path}...")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_parquet(out_path, index=False)
    
    print(f"[VALIDATE] Completed validation process successfully. Output saved to {out_path}")

if __name__ == "__main__":
    # Allow execution with default CLI arguments
    input_file = sys.argv[1] if len(sys.argv) > 1 else "/tmp/raw_orders.parquet"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "/tmp/validated_orders.parquet"
    validate(input_file, output_file)
