from mcp.server.fastmcp import FastMCP
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv()

mcp = FastMCP("PythonServer", log_level="INFO")

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

class T1Output(BaseModel):
    product_id: str = Field(..., description="Product/Model ID to be transferred")
    requesting_loc_id: str = Field(..., description="Receiving location ID (Dealer etc.)")
    supplying_loc_id: str = Field(..., description="Sending location ID (Plant, Hub etc.)")

class T2Output(BaseModel):
    product_id: str = Field(..., description="Product/Model ID to be produced")
    requested_qty: int = Field(..., description="Requested production quantity")
    due_date: str = Field(..., description="Requested completion or delivery date")

class T3Output(BaseModel):
    product_id: str = Field(..., description="Product/Model ID")
    target_period: str = Field(..., description="Forecast period (date or week etc.)")
    upcoming_campaigns: str = Field(..., description="Relevant marketing campaigns in period")
    
task_map = {
    "t1": T1Output,
    "t2": T2Output,
    "t3": T3Output,
}

@mcp.tool()
def user_intent_parser(task: str, query: str) -> Dict[str, Any]:
    """
    Parses a natural language user query into a structured output according to the specified task type.
    """
    output_schema = task_map.get(task)
    if output_schema is None:
        raise ValueError(f"Unsupported task.")

    llm_structured = llm.with_structured_output(output_schema)
    result = llm_structured.invoke(query)
    return result.model_dump()

if __name__ == "__main__":
    mcp.run(transport="stdio")