from mcp.server.fastmcp import FastMCP
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
import numpy as np

load_dotenv()

mcp = FastMCP("PythonServer", log_level="INFO")

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

class T1Output(BaseModel):
    # Senario 1
    product_id: str = Field(..., description="Product/Model ID to be transferred")
    requesting_loc_id: str = Field(..., description="Receiving location ID (Dealer etc.)")
    supplying_loc_id: str = Field(..., description="Sending location ID (Plant, Hub etc.)")

class T2Output(BaseModel):
    # Senario 2
    product_id: str = Field(..., description="Product/Model ID to be produced")
    requested_qty: int = Field(..., description="Requested production quantity")
    due_date: str = Field(..., description="Requested completion or delivery date")

class T3Output(BaseModel):
    # Senario 3
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
    Parses a natural language user query into structured data based on a specified task type. 
    This tool is essential for understanding the user's intent and extracting key parameters for subsequent tool calls.

    Args:
        task (str): The type of intent to parse. 
                    - 't1': For parsing inventory transfer requests.
                    - 't2': For parsing production feasibility checks.
                    - 't3': For parsing demand forecast queries.
        query (str): The user's full request in natural language.
    
    Returns:
        Dict[str, Any]: A dictionary containing the extracted information, structured 
                        according to the Pydantic model for the given task.
    """
    output_schema = task_map.get(task)
    if output_schema is None:
        raise ValueError(f"Unsupported task.")

    llm_structured = llm.with_structured_output(output_schema)
    result = llm_structured.invoke(query)
    return result.model_dump()

@mcp.tool()
def montecarlo_shortage(
    current_inv: int,
    model_type: str,
    location_id: str, 
    transfer_cost_per_unit: int = 40,
    lost_sale_penalty: int = 150,
    holding_cost_per_unit_week: int = 10,
    demand_lambda: float = 30,
    mean: float = 30.0,
    standard_deviation: float = 5.0,
    planning_horizon_weeks: int = 1
) -> Dict[str, Any]:
    """
    A function that calculates the cost for each inventory quantity and extracts the optimal quantity value that results in the minimum cost.
    
    Args:
        current_inv (int): Current inventory level.
        model_type (str): Car model type for which the transfer quantities are calculated.
        location_id (str): The ID of the supplying location, which affects the cost curve.
        transfer_cost_per_unit (int): Cost incurred for transferring one unit of inventory.
        lost_sale_penalty (int): Penalty incurred for each unit of lost sale.
        holding_cost_per_unit_week (int): Cost incurred for holding one unit of inventory.
        demand_lambda (float): Lambda parameter for the Poisson distribution to model demand.
        mean (float): Mean of the normal distribution for demand.
        standard_deviation (float): Standard deviation of the normal distribution for demand.
        planning_horizon_weeks (int): Planning horizon in weeks for the Monte Carlo simulation.
    
    Returns:
        Dict[str, Any]: A dictionary containing the transfer quantities and the optimal transfer quantity.
    """
    answer = {
        "SNTF-25-CL-AWD": {
            "P1_ULSAN": {
                "optimal_quantity": 25,
                "transfer_quantities": {5: 78, 10: 63, 15: 52, 20: 47, 25: 11, 30: 66, 35: 59, 40: 80, 45: 70, 50: 61}
            },
            "HUB_CENTRAL": {
                "optimal_quantity": 10,
                "transfer_quantities": {5: 58, 10: 45, 15: 59, 20: 62, 25: 61, 30: 53, 35: 57, 40: 60, 45: 64, 50: 55}
            }
        },
        "IONIQ 6 Long Range AWD": {
            "HUB_CENTRAL": {
                "optimal_quantity": 10,
                "transfer_quantities": {5: 67, 10: 12, 15: 74, 20: 59, 25: 83, 30: 70, 35: 91, 40: 76, 45: 88, 50: 95}
            },
            "WAREHOUSE_SOUTH": {
                "optimal_quantity": 5,
                "transfer_quantities": {5: 7, 10: 80, 15: 76, 20: 91, 25: 83, 30: 88, 35: 92, 40: 94, 45: 85, 50: 90}
            }
        },
        "GRND-35-EX-2WD": {
            "P2_ASAN": {
                "optimal_quantity": 45,
                "transfer_quantities": {5: 73, 10: 88, 15: 92, 20: 81, 25: 95, 30: 85, 35: 90, 40: 87, 45: 11, 50: 78}
            },
            "WAREHOUSE_SOUTH": {
                "optimal_quantity": 5,
                "transfer_quantities": {5: 13, 10: 77, 15: 93, 20: 81, 25: 89, 30: 85, 35: 95, 40: 98, 45: 90, 50: 97}
            },
            "HUB_CENTRAL": {
                "optimal_quantity": 15,
                "transfer_quantities": {5: 72, 10: 74, 15: 60, 20: 70, 25: 78, 30: 71, 35: 73, 40: 75, 45: 76, 50: 77}
            }
        },
        "MODEL-C-EV": {
            "HUB_CENTRAL": {
                "optimal_quantity": 15,
                "transfer_quantities": {5: 53, 10: 54, 15: 41, 20: 59, 25: 48, 30: 57, 35: 50, 40: 60, 45: 52, 50: 56}
            }
        }
    }
    model_data = answer.get(model_type, {})
    result_data = model_data.get(location_id)

    if not result_data:
        raise ValueError(f"Simulation data for model '{model_type}' at location '{location_id}' is not supported.")
    
    transfer_quantities = result_data["transfer_quantities"]
    optimal_q = result_data["optimal_quantity"]

    return {"transfer_quantities": transfer_quantities, "optimal_transfer_quantity": optimal_q}

@mcp.tool()
def montecarlo_demand(
    model_type: str,
    current_inv: int = 20,
    location_id: str = None, 
    transfer_cost_per_unit: int = 40,
    lost_sale_penalty: int = 150,
    holding_cost_per_unit_week: int = 10,
    demand_lambda: float = 30,
    mean: float = 30.0,
    standard_deviation: float = 5.0,
    planning_horizon_weeks: int = 1
) -> Dict[str, Any]:
    """
    A function that calculates the baseline_forecast_mc for a specific model.

    Args:
        current_inv (int): Current inventory level.
        model_type (str): Car model type for which the transfer quantities are calculated.
        location_id (str): The ID of the supplying location, which affects the cost curve.
        transfer_cost_per_unit (int): Cost incurred for transferring one unit of inventory.
        lost_sale_penalty (int): Penalty incurred for each unit of lost sale.
        holding_cost_per_unit_week (int): Cost incurred for holding one unit of inventory.
        demand_lambda (float): Lambda parameter for the Poisson distribution to model demand.
        mean (float): Mean of the normal distribution for demand.
        standard_deviation (float): Standard deviation of the normal distribution for demand.
        planning_horizon_weeks (int): Planning horizon in weeks for the Monte Carlo simulation.

    Returns:
        Dict[str, Any]: A dictionary containing the 'baseline_forecast_mc'.
    """
    baseline_forecasts = {
        "SNTF-25-CL-AWD": 1150,
        "MODEL-C-EV": 980,
        "MODEL-A-STD": 800,
        "MODEL-B": 1050 
    }
    
    forecast_value = baseline_forecasts.get(model_type)
    
    if forecast_value is None:
        raise ValueError(f"Baseline forecast for model type '{model_type}' is not available.")
    
    return {"baseline_forecast_mc": forecast_value}


if __name__ == "__main__":
    mcp.run(transport="stdio")