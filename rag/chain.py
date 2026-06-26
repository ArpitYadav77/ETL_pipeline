# Part of: E-commerce ETL Pipeline & RAG Support Assistant
"""
Chain module for the RAG System.
Configures and initializes the RetrievalQA chain combining the ChromaDB retriever
and ChatOpenAI model using a custom prompt.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
from langchain_community.vectorstores import Chroma

# Load environment variables
load_dotenv()

def get_qa_chain():
    """
    Initializes and returns a LangChain RetrievalQA chain configured with the
    persisted Chroma vector store and OpenAI's GPT-3.5 Turbo model.
    
    Returns:
        RetrievalQA: Configured RetrievalQA chain instance.
    """
    print("[CHAIN] Initializing QA Chain...")
    
    # 1. Verify Gemini API key
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key or api_key == "your-gemini-key-here":
        raise ValueError("GEMINI_API_KEY is not set or contains placeholder template value in .env")
        
    # 2. Define path to ChromaDB
    project_root = Path(__file__).resolve().parent.parent
    persist_dir = project_root / "chroma_store"
    
    if not persist_dir.exists():
        raise FileNotFoundError(f"ChromaDB persist directory not found at {persist_dir}. Run the embedding script first.")
        
    # 3. Load embeddings model
    print("[CHAIN] Loading GoogleGenerativeAIEmbeddings with models/gemini-embedding-001...")
    embeddings = GoogleGenerativeAIEmbeddings(
        model="models/gemini-embedding-001",
        google_api_key=api_key
    )
    
    # 4. Load persisted ChromaDB
    print(f"[CHAIN] Loading Chroma DB from {persist_dir}...")
    vectordb = Chroma(
        persist_directory=str(persist_dir),
        embedding_function=embeddings
    )
    
    # 5. Create retriever with k=5 and similarity search
    print("[CHAIN] Creating retriever (k=5, search_type=similarity)...")
    retriever = vectordb.as_retriever(
        search_type="similarity",
        search_kwargs={"k": 5}
    )
    
    # 6. Define custom prompt template matching exact requirements
    print("[CHAIN] Constructing PromptTemplate...")
    prompt_template = """You are a business analyst assistant for an e-commerce company. Answer questions using ONLY the context provided. Always cite which data source you used (monthly_revenue, top_products, top_customers, or return_policy). If the answer is not in the context, say 'I don't have that information in my current data.' Be concise and specific.

Context:
{context}

Question: {question}
Answer:"""

    prompt = PromptTemplate(
        template=prompt_template,
        input_variables=["context", "question"]
    )
    
    # 7. Initialize Chat LLM
    print("[CHAIN] Initializing ChatGoogleGenerativeAI (models/gemini-2.5-flash)...")
    llm = ChatGoogleGenerativeAI(
        model="models/gemini-2.5-flash",
        temperature=0.0,
        google_api_key=api_key
    )
    
    # 8. Create RetrievalQA chain
    print("[CHAIN] Building RetrievalQA chain...")
    qa_chain = RetrievalQA.from_chain_type(
        llm=llm,
        chain_type="stuff",
        retriever=retriever,
        return_source_documents=True,
        chain_type_kwargs={"prompt": prompt}
    )
    
    print("[CHAIN] QA Chain initialized successfully.")
    return qa_chain

if __name__ == "__main__":
    try:
        chain = get_qa_chain()
        print("QA Chain loaded successfully.")
    except Exception as e:
        print(f"Error loading QA Chain: {e}")
