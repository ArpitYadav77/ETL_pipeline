# Part of: E-commerce ETL Pipeline & RAG Support Assistant
"""
Transform module for the E-commerce ETL Pipeline.
Cleans columns, calculates revenue, and generates aggregated tables.
"""

import sys
from pathlib import Path
import pandas as pd

def transform(input_path: str = "/tmp/validated_orders.parquet", output_dir: str = "/tmp") -> None:
    """
    Reads the validated parquet file, parses InvoiceDate, cleans descriptions,
    converts CustomerID to int, calculates revenue, creates four aggregated datasets,
    and writes them as parquet files.
    
    Args:
        input_path (str): Path to the validated parquet file.
        output_dir (str): Directory where the output parquet files will be written.
    """
    print(f"[TRANSFORM] Starting transform process on {input_path}...")
    
    in_path = Path(input_path)
    out_directory = Path(output_dir)
    
    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found at {in_path}")
        
    print(f"[TRANSFORM] Reading validated parquet from {in_path}...")
    df = pd.read_parquet(in_path)
    print(f"[TRANSFORM] Loaded {len(df)} rows for transformation.")
    
    # 1. Parse InvoiceDate as datetime
    print("[TRANSFORM] Parsing InvoiceDate as datetime...")
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    
    # 2. Strip and lowercase Description
    print("[TRANSFORM] Cleaning Description column...")
    df["Description"] = df["Description"].astype(str).str.strip().str.lower()
    
    # 3. Convert CustomerID to int
    print("[TRANSFORM] Converting CustomerID to integer...")
    # CustomerID might be stored as float in raw pandas representation (e.g. 17850.0)
    df["CustomerID"] = df["CustomerID"].astype(float).astype(int)
    
    # 4. Compute Revenue = Quantity * UnitPrice
    print("[TRANSFORM] Calculating Revenue column...")
    df["Revenue"] = df["Quantity"] * df["UnitPrice"]
    
    # Create the four aggregated dataframes
    
    # 1. df_orders — the full cleaned orders table
    print("[TRANSFORM] Creating full cleaned orders table...")
    df_orders = df.copy()
    
    # 2. df_monthly — columns: month (YYYY-MM), total_revenue (sum), order_count (unique InvoiceNo)
    print("[TRANSFORM] Creating monthly revenue aggregation...")
    df["month"] = df["InvoiceDate"].dt.strftime("%Y-%m")
    df_monthly = df.groupby("month").agg(
        total_revenue=("Revenue", "sum"),
        order_count=("InvoiceNo", "nunique")
    ).reset_index()
    
    # 3. df_customers — columns: CustomerID, total_revenue, order_count (unique InvoiceNo) — top 500 customers by revenue
    print("[TRANSFORM] Creating top customers aggregation...")
    df_customers = df.groupby("CustomerID").agg(
        total_revenue=("Revenue", "sum"),
        order_count=("InvoiceNo", "nunique")
    ).reset_index()
    df_customers = df_customers.sort_values(by="total_revenue", ascending=False).head(500)
    
    # 4. df_products — columns: description, total_quantity, total_revenue — top 200 products by revenue
    print("[TRANSFORM] Creating top products aggregation...")
    # Rename Description to description for columns matching requirement: description, total_quantity, total_revenue
    df_products = df.groupby("Description").agg(
        total_quantity=("Quantity", "sum"),
        total_revenue=("Revenue", "sum")
    ).reset_index()
    df_products = df_products.rename(columns={"Description": "description"})
    df_products = df_products.sort_values(by="total_revenue", ascending=False).head(200)
    
    # Ensure output directory exists
    out_directory.mkdir(parents=True, exist_ok=True)
    
    # Paths for output parquet files
    path_orders = out_directory / "raw_orders.parquet"
    path_monthly = out_directory / "monthly_revenue.parquet"
    path_customers = out_directory / "top_customers.parquet"
    path_products = out_directory / "top_products.parquet"
    
    # Save datasets
    print(f"[TRANSFORM] Saving output parquet files to {out_directory}...")
    df_orders.to_parquet(path_orders, index=False)
    df_monthly.to_parquet(path_monthly, index=False)
    df_customers.to_parquet(path_customers, index=False)
    df_products.to_parquet(path_products, index=False)
    
    # Print summary statistics
    print("\n=== TRANSFORM SUMMARY STATS ===")
    print(f"Full Cleaned Orders: {len(df_orders)} rows, total revenue: £{df_orders['Revenue'].sum():,.2f}")
    print(f"Monthly Revenue (sample): {len(df_monthly)} months. Total revenue in data: £{df_monthly['total_revenue'].sum():,.2f}")
    print(f"Top Customers (sample): {len(df_customers)} customers loaded. Top customer revenue: £{df_customers['total_revenue'].iloc[0]:,.2f}")
    print(f"Top Products (sample): {len(df_products)} products loaded. Top product: '{df_products['description'].iloc[0]}' with revenue £{df_products['total_revenue'].iloc[0]:,.2f}")
    print("===============================\n")
    
    print(f"[TRANSFORM] Completed transform process successfully. Outputs saved in {out_directory}")

if __name__ == "__main__":
    input_file = sys.argv[1] if len(sys.argv) > 1 else "/tmp/validated_orders.parquet"
    output_folder = sys.argv[2] if len(sys.argv) > 2 else "/tmp"
    transform(input_file, output_folder)
