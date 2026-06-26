# Part of: E-commerce ETL Pipeline & RAG Support Assistant
"""
Embed module for the RAG System.
Converts textual documents into embeddings using OpenAI's text-embedding-3-small,
and persists the resulting vectors to ChromaDB at `./chroma_store`.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from langchain_core.documents import Document
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_community.vectorstores import Chroma

import sys
from pathlib import Path

# Add project root to sys.path so that 'rag' can be imported when run as a script
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# Import generate_documents
from rag.generate_docs import generate_documents

# Load environment variables
load_dotenv()

def embed_documents() -> None:
    """
    Retrieves all text documents, classifies their source, creates LangChain
    Document objects with appropriate metadata, and embeds them into ChromaDB.
    """
    print("[EMBED] Starting embed_documents process...")
    
    # 1. Retrieve raw text documents
    raw_texts = generate_documents()
    if not raw_texts:
        print("[EMBED] No documents found to embed. Run the ETL pipeline first.")
        return
        
    print(f"[EMBED] Retrieved {len(raw_texts)} text documents. Preparing metadata mapping...")
    
    # 2. Wrap text in Document objects with source metadata
    documents = []
    for text in raw_texts:
        # Determine source based on formatting pattern
        if text.startswith("In ") and "orders." in text:
            source = "monthly_revenue"
        elif text.startswith("Product '"):
            source = "top_products"
        elif text.startswith("Customer ") and "revenue across" in text:
            source = "top_customers"
        else:
            source = "return_policy"
            
        doc = Document(page_content=text, metadata={"source": source})
        documents.append(doc)
        
    # 3. Verify Gemini API key
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key or api_key == "your-gemini-key-here":
        raise ValueError("GEMINI_API_KEY is not set or contains the placeholder template value in .env")
        
    # 4. Initialize Gemini embeddings
    print("[EMBED] Initializing GoogleGenerativeAIEmbeddings with model: models/gemini-embedding-001...")
    embeddings = GoogleGenerativeAIEmbeddings(
        model="models/gemini-embedding-001",
        google_api_key=api_key
    )
    
    # 5. Create and persist Chroma vector store in batches to avoid Gemini rate limits
    project_root = Path(__file__).resolve().parent.parent
    persist_dir = project_root / "chroma_store"
    
    batch_size = 50
    print(f"[EMBED] Initializing Chroma vector store at {persist_dir} with first batch of {batch_size} documents...")
    vectordb = Chroma.from_documents(
        documents=documents[:batch_size],
        embedding=embeddings,
        persist_directory=str(persist_dir)
    )
    
    import time
    for i in range(batch_size, len(documents), batch_size):
        chunk = documents[i : i + batch_size]
        print(f"[EMBED] Adding batch {i} to {i + len(chunk)} to vector store...")
        vectordb.add_documents(chunk)
        print("[EMBED] Sleeping for 6 seconds to avoid rate limits...")
        time.sleep(6)
        
    # Persist database to disk
    print("[EMBED] Persisting vector database...")
    vectordb.persist()
    
    print(f"[EMBED] Embedded {len(documents)} documents into ChromaDB successfully.")

if __name__ == "__main__":
    embed_documents()
