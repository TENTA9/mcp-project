from mcp.server.fastmcp import FastMCP
from typing import List, Dict, Any
from langchain_openai import OpenAIEmbeddings
from langchain_chroma import Chroma
from dotenv import load_dotenv
import os

load_dotenv()

mcp = FastMCP("VectorDBServer_Chroma", log_level="INFO")

embeddings = OpenAIEmbeddings(
    model='text-embedding-3-small',
    openai_api_key=os.getenv("OPENAI_API_KEY")
)

db_directory = "./data/chroma_db"
vector_db = Chroma(
    persist_directory=db_directory,
    embedding_function=embeddings
)

# --- 툴 추가 ---
# 예: @mcp.tool()
#     def search_supply_chain_docs(query: str) -> str:
#         results = vector_db.similarity_search(query, k=3)
#         return "\n".join([doc.page_content for doc in results])

if __name__ == "__main__":
    mcp.run(transport="stdio")