from mcp.server.fastmcp import FastMCP
from typing import List, Dict, Any
import requests

mcp = FastMCP("ExternalAPIServer", log_level="INFO")

# --- 여기에 외부 API의 기본 URL, 인증키 등 설정 ---
# 예: ERP_API_BASE_URL = "https://api.erp.com/v1"
#     ERP_API_KEY = "..."

# --- 여기에 외부 API 연동 툴들을 추가 ---
# 예: @mcp.tool()
#     def create_erp_purchase_order(...) -> ...:
#         ...

if __name__ == "__main__":
    mcp.run(transport="stdio")