# Part of: E-commerce ETL Pipeline & RAG Support Assistant
"""
Generate Docs module for the RAG System.
Retrieves metrics from the PostgreSQL database, formats them as human-readable sentences,
reads and chunks the return policy document, and returns them as a combined list of texts.
"""

import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def chunk_text(text: str, chunk_size: int = 400, overlap: int = 40) -> list[str]:
    """
    Splits string text into chunks of chunk_size characters with overlap overlap.
    
    Args:
        text (str): The raw text to split.
        chunk_size (int): Character size of each chunk.
        overlap (int): Overlap character size between consecutive chunks.
        
    Returns:
        list[str]: Chunks of text.
    """
    if chunk_size <= overlap:
        raise ValueError("chunk_size must be greater than overlap.")
        
    chunks = []
    start = 0
    text_len = len(text)
    
    while start < text_len:
        end = min(start + chunk_size, text_len)
        chunks.append(text[start:end])
        # If we reached the end of the string, break
        if end == text_len:
            break
        start += (chunk_size - overlap)
        
    return chunks

def generate_documents() -> list[str]:
    """
    Queries monthly_revenue, top_products, and top_customers from PostgreSQL,
    converts rows into formatted text documents, reads and chunks return_policy.txt,
    and returns a combined list of all documents.
    
    Returns:
        list[str]: Combined list of document strings.
    """
    print("[GENERATE_DOCS] Starting generate_documents process...")
    
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL environment variable is not set in .env")
        
    engine = create_engine(db_url)
    documents = []
    
    # Query databases
    print("[GENERATE_DOCS] Connecting to PostgreSQL to retrieve metrics...")
    with engine.connect() as conn:
        # 1. Query monthly_revenue
        print("[GENERATE_DOCS] Querying 'monthly_revenue' table...")
        df_monthly = pd.read_sql("SELECT month, total_revenue, order_count FROM monthly_revenue", conn)
        for _, row in df_monthly.iterrows():
            month = row["month"]
            total_rev = float(row["total_revenue"])
            order_cnt = int(row["order_count"])
            doc = f"In {month}, total revenue was £{total_rev:,.2f} across {order_cnt} orders."
            documents.append(doc)
            
        # 2. Query top_products
        print("[GENERATE_DOCS] Querying 'top_products' table...")
        df_products = pd.read_sql("SELECT description, total_quantity, total_revenue FROM top_products LIMIT 20", conn)
        for _, row in df_products.iterrows():
            desc = row["description"]
            total_qty = int(row["total_quantity"])
            total_rev = float(row["total_revenue"])
            doc = f"Product '{desc}' sold {total_qty} units with total revenue of £{total_rev:,.2f}."
            documents.append(doc)
            
        # 3. Query top_customers
        print("[GENERATE_DOCS] Querying 'top_customers' table...")
        df_customers = pd.read_sql('SELECT "CustomerID", total_revenue, order_count FROM top_customers LIMIT 20', conn)
        for _, row in df_customers.iterrows():
            cust_id = int(float(row["CustomerID"]))
            total_rev = float(row["total_revenue"])
            order_cnt = int(row["order_count"])
            doc = f"Customer {cust_id} generated £{total_rev:,.2f} in revenue across {order_cnt} orders."
            documents.append(doc)
            
    print(f"[GENERATE_DOCS] Generated {len(documents)} document strings from database tables.")
    
    # 4. Load return policy
    # Find return policy relative to project root
    project_root = Path(__file__).resolve().parent.parent
    policy_path = project_root / "policies" / "return_policy.txt"
    
    print(f"[GENERATE_DOCS] Reading return policy from {policy_path}...")
    if not policy_path.exists():
        raise FileNotFoundError(f"Return policy document not found at {policy_path}")
        
    with open(policy_path, "r", encoding="utf-8") as f:
        policy_text = f.read()
        
    policy_chunks = chunk_text(policy_text, chunk_size=400, overlap=40)
    print(f"[GENERATE_DOCS] Chunked return policy into {len(policy_chunks)} sections.")
    
    # Append policy chunks to documents
    documents.extend(policy_chunks)
    
    total_docs = len(documents)
    print(f"[GENERATE_DOCS] Completed generate_documents process. Total documents: {total_docs}")
    return documents

if __name__ == "__main__":
    docs = generate_documents()
    print(f"Sample Document: {docs[0] if docs else 'None'}")
