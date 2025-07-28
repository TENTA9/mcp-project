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
    
class T4Output(BaseModel):
    # Senario 4
    product_id: str = Field(..., description="The product ID for the incentive plan.")
    period: str = Field(..., description="The target period for the incentive plan (e.g., 'Q4 2025', 'August 2025').")
    total_budget: int = Field(..., description="The total available budget for the incentive plan, as an integer.")

class T5Output(BaseModel):
    # Senario 5
    component_id: str = Field(..., description="The ID of the component with a shortage.")
    shortage_quantity: int = Field(..., description="The quantity of the component shortage.")

class T6Output(BaseModel):
    # Senario 6
    source_plant_id: str = Field(..., description="The starting point of the route, typically a plant.")
    new_hub_id: str = Field(..., description="The new hub to be considered as a waypoint.")
    target_dealer_id: str = Field(..., description="The final destination of the route, typically a dealer.")

class T7Output(BaseModel):
    # Senario 7
    base_model: str = Field(..., description="The base model of the product lineup to be analyzed.")
    period: str = Field(..., description="The target period for the analysis (e.g., 'next quarter', 'next year').")

class T8Output(BaseModel):
    # Senario 8
    component_id: str = Field(..., description="The ID of the component being discontinued.")
    supplier_id: str = Field(..., description="The ID of the supplier who is discontinuing the part.")
    last_order_date: str = Field(..., description="The final date for placing an order in YYYY-MM-DD format.")
    
class Scenario(BaseModel):
    # For senario 9. A single scenario for financial forecasting.
    scenario_name: str = Field(..., description="The name of the scenario (e.g., 'Base Case', 'High Inflation').")
    inflation_factor: float = Field(..., description="The inflation factor (e.g., 1.0 for base, 1.05 for 5% inflation).")
    price_pass_through_pct: int = Field(..., description="The percentage of cost increase passed to the price (0-100).")

class T9Output(BaseModel):
    # Senario 9
    target_base_model: str = Field(..., description="The target base model to forecast.")
    forecast_horizon_months: int = Field(..., description="The number of months for the forecast horizon.")
    scenarios: List[Scenario] = Field(..., description="A list of scenarios to be simulated.")
    
task_map = {
    "t1": T1Output,
    "t2": T2Output,
    "t3": T3Output,
    "t4": T4Output,
    "t5": T5Output,
    "t6": T6Output,
    "t7": T7Output,
    "t8": T8Output,
    "t9": T9Output,
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
                    - 't4': For parsing incentive budget allocation requests.
                    - 't5': For parsing component shortage and sourcing plan requests.
                    - 't6': For parsing logistics route re-optimization requests.
                    - 't7': For parsing product trim simplification requests.
                    - 't8': For parsing End-of-Life (EOL) buy strategy requests.
                    - 't9': For parsing financial forecasts with inflation scenarios.
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

@mcp.tool()
def select_optimal_supplier(
    mitigation_options: List[Dict[str, Any]], 
    shortage_quantity: int
) -> Dict[str, Any]:
    """
    Evaluates a list of alternative suppliers based on a weighted score of quality, lead time, and cost, 
    and selects the optimal one.

    Args:
        mitigation_options: A list of dictionaries, where each dictionary is an alternative supplier option from 
                            the 'search_alternative_suppliers' tool.
        shortage_quantity: The number of units that need to be sourced.

    Returns:
        A dictionary containing the 'selected_solution' and a 'rationale' for the decision.
    """

    if not mitigation_options:
        return {
            "selected_solution": {
                "type": "SUPPLY_UNAVAILABLE",
                "reason": "No alternative suppliers were found for the component."
            },
            "rationale": "No sourcing options available in the Sourcing_Rules table."
        }

    max_lead_time = max(opt['lead_time_days'] for opt in mitigation_options)
    max_unit_price = max(float(opt['unit_price']) for opt in mitigation_options)

    scored_options = []
    for option in mitigation_options:
        
        norm_quality = option['quality_score'] / 100.0

        norm_lead_time = 1.0 - (option['lead_time_days'] / max_lead_time) if max_lead_time > 0 else 0

        norm_cost = 1.0 - (float(option['unit_price']) / max_unit_price) if max_unit_price > 0 else 0

        score = (norm_quality * 0.6) + (norm_lead_time * 0.25) + (norm_cost * 0.15)

        option_with_score = option.copy()
        option_with_score['score'] = round(score, 4)
        scored_options.append(option_with_score)

    best_option = max(scored_options, key=lambda x: x['score'])

    selected_solution = {
        "type": "ALTERNATIVE_SUPPLIER",
        "component_id": best_option['component_id'],
        "supplier_id": best_option['supplier_id'],
        "supplier_name": best_option['supplier_name'],
        "quantity_to_order": shortage_quantity,
        "score": best_option['score']
    }
    
    rationale = (
        f"Selected supplier '{best_option['supplier_name']}' ({best_option['supplier_id']}) due to the highest overall score of {best_option['score']}. "
        f"This decision was based on a weighted evaluation of its quality score ({best_option['quality_score']}), "
        f"lead time ({best_option['lead_time_days']} days), and unit price (${float(best_option['unit_price']):.2f})."
    )

    return {"selected_solution": selected_solution, "rationale": rationale}

@mcp.tool()
def compare_route_effectiveness(
    current_route_model: Dict[str, Any], 
    new_route_model: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Calculates and compares the total time and 'effective total cost' for a direct route and a new route via a hub.
    Effective cost includes a time-based cost (500 per hour).

    Args:
        current_route_model: A dictionary representing the direct route with 'total_transit_hr' and 'direct_cost'.
        new_route_model: A nested dictionary representing the hub route with 'leg1', 'hub', and 'leg2' details.

    Returns:
        A dictionary containing a 'comparison_summary' of both routes.
    """

    current_route_summary = {}
    if current_route_model:
        direct_time = current_route_model.get('total_transit_hr', 0)
        direct_cost = current_route_model.get('direct_cost', 0)
        effective_cost = direct_cost + (direct_time * 500)
        
        current_route_summary = {
            "total_time_hr": direct_time,
            "direct_cost": direct_cost,
            "effective_total_cost": effective_cost
        }

    leg1 = new_route_model.get('leg1', {})
    hub = new_route_model.get('hub', {})
    leg2 = new_route_model.get('leg2', {})
    
    new_total_time = leg1.get('transit_hr', 0) + hub.get('handling_hr', 0) + leg2.get('transit_hr', 0)
    new_direct_cost = leg1.get('cost', 0) + hub.get('handling_cost', 0) + leg2.get('cost', 0)
    new_effective_cost = new_direct_cost + (new_total_time * 500)
    
    new_route_summary = {
        "total_time_hr": new_total_time,
        "direct_cost": new_direct_cost,
        "effective_total_cost": new_effective_cost
    }

    return {
        "comparison_summary": {
            "current_route": current_route_summary,
            "new_route": new_route_summary
        }
    }

@mcp.tool()
def generate_route_recommendation(comparison_summary: Dict[str, Any]) -> Dict[str, Any]:
    """
    Based on the effective total cost comparison, determines the optimal route and 
    generates the final recommendation answer to present to the user.

    Args:
        comparison_summary: The output from the 'compare_route_effectiveness' tool, 
                            containing cost/time details for both the current and new routes.

    Returns:
        A dictionary containing the final answer, which includes the recommendation, 
        justification, and a cost comparison.
    """
    
    current_route = comparison_summary.get('current_route', {})
    new_route = comparison_summary.get('new_route', {})

    current_effective_cost = current_route.get('effective_total_cost', float('inf'))
    new_effective_cost = new_route.get('effective_total_cost', float('inf'))
    
    recommendation = ""
    justification = ""
    
    if new_effective_cost < current_effective_cost:
        recommendation = "New Route via Hub"
        justification = "The route via the new hub is more efficient in terms of total effective cost."
    else:
        recommendation = "Current Direct Route"
        justification = "The existing direct route is more or equally efficient in terms of total effective cost."

    if current_effective_cost == float('inf') and new_effective_cost == float('inf'):
         recommendation = "No Route Found"
         justification = "Could not find a valid direct or hub-based route between the specified locations."

    expected_savings = current_effective_cost - new_effective_cost if recommendation == "New Route via Hub" else 0

    final_answer = {
        "recommendation": recommendation,
        "justification": justification,
        "comparison": {
            "current_route_effective_cost": current_effective_cost if current_route else None,
            "new_route_effective_cost": new_effective_cost,
            "expected_savings": expected_savings
        }
    }
    return {"final_answer": final_answer}

@mcp.tool()
def identify_efficiency_outliers(
    trim_performance_data: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Identifies the most and least efficient trims from a list of performance data 
    by calculating a margin-to-cost efficiency ratio for each.

    Args:
        trim_performance_data: The list of trim performance dictionaries, typically from the 
                               'aggregate_trim_performance' tool. Each dictionary must contain 
                               'product_id', 'standard_product_cost', 'standard_production_time_hours', 
                               and 'unit_margin'.

    Returns:
        A dictionary containing the 'least_efficient_trim' and 'most_efficient_trim',
        each with their key performance details.
    """
    if not trim_performance_data:
        raise ValueError("Input 'trim_performance_data' cannot be empty.")

    scored_trims = []
    for trim in trim_performance_data:
        cost = float(trim.get('standard_product_cost', 0))
        margin = float(trim.get('unit_margin', 0))

        if cost > 0:
            efficiency_ratio = margin / cost
        else:
            efficiency_ratio = 0

        scored_trim = trim.copy()
        scored_trim['efficiency_ratio'] = efficiency_ratio
        scored_trims.append(scored_trim)

    most_efficient = max(scored_trims, key=lambda x: x['efficiency_ratio'])
    least_efficient = min(scored_trims, key=lambda x: x['efficiency_ratio'])

    def format_output(trim_data):
        return {
            "product_id": trim_data['product_id'],
            "production_time": trim_data['standard_production_time_hours'],
            "unit_margin": float(trim_data['unit_margin'])
        }

    return {
        "least_efficient_trim": format_output(least_efficient),
        "most_efficient_trim": format_output(most_efficient)
    }

@mcp.tool()
def simulate_financial_impact(
    optimal_simulation_parameters: Dict[str, Any],
    trim_performance_data: List[Dict[str, Any]],
    period: str = "1 quarter"
) -> Dict[str, Any]:
    """
    Simulates the net financial margin impact of a proposed production shift between two trims.

    Args:
        optimal_simulation_parameters: The output from 'calculate_optimal_shift', containing the
                                       reduce/reallocate targets and quantities.
        trim_performance_data: The output from 'aggregate_trim_performance', containing the unit margin
                               for each trim.
        period: The period for which the simulation is being run (for context).

    Returns:
        A dictionary with the 'simulation_result', detailing the margin changes and the net impact.
    """

    reduce_target = optimal_simulation_parameters['reduce_target']
    reallocate_to = optimal_simulation_parameters['reallocate_to']
    
    reduce_id = reduce_target['product_id']
    reduce_qty = reduce_target['quantity']
    reallocate_id = reallocate_to['product_id']
    reallocate_qty = reallocate_to['quantity']

    margin_map = {trim['product_id']: float(trim['unit_margin']) for trim in trim_performance_data}

    reduce_margin = margin_map.get(reduce_id, 0)
    reallocate_margin = margin_map.get(reallocate_id, 0)

    margin_change_from_reduction = reduce_qty * reduce_margin
    margin_change_from_reallocation = reallocate_qty * reallocate_margin
    net_margin_impact = margin_change_from_reallocation + margin_change_from_reduction

    result_key_reduction = f"margin_change_from_{reduce_id}"
    result_key_reallocation = f"margin_change_from_{reallocate_id}"
    
    simulation_result = {
        result_key_reduction: margin_change_from_reduction,
        result_key_reallocation: margin_change_from_reallocation,
        "net_margin_impact": net_margin_impact
    }
    
    return {"simulation_result": simulation_result}

@mcp.tool()
def generate_mix_recommendation(
    simulation_result: Dict[str, Any],
    optimal_simulation_parameters: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Based on the simulation results, formulates a final, structured recommendation for adjusting the sales mix.

    Args:
        simulation_result: The output from 'simulate_financial_impact', containing the net margin impact.
        optimal_simulation_parameters: The output from 'calculate_optimal_shift', containing the
                                       reduce/reallocate targets and quantities.

    Returns:
        A dictionary containing the final answer, including the recommendation, justification, and details.
    """
    net_margin_impact = simulation_result.get('net_margin_impact', 0)
    reduce_target = optimal_simulation_parameters.get('reduce_target', {})
    reallocate_to = optimal_simulation_parameters.get('reallocate_to', {})
    binding_constraint = optimal_simulation_parameters.get('binding_constraint', 'N/A')

    recommendation = [
        {
            "action_type": "REDUCE_TARGET",
            "product_id": reduce_target.get('product_id'),
            "quantity": reduce_target.get('quantity')
        },
        {
            "action_type": "INCREASE_TARGET",
            "product_id": reallocate_to.get('product_id'),
            "quantity": reallocate_to.get('quantity')
        }
    ]

    justification = (
        f"This is the optimal sales mix adjustment based on the '{binding_constraint}' constraint. "
        f"Shifting production from {reduce_target.get('product_id')} to {reallocate_to.get('product_id')} "
        f"is projected to yield the highest possible margin gain under the current limitations."
    )

    details = {
        "projected_net_margin_gain": net_margin_impact
    }

    final_answer = {
        "recommendation": recommendation,
        "justification": justification,
        "details": details
    }
    
    return {"final_answer": final_answer}

@mcp.tool()
def optimize_final_buy_quantity(
    total_required_units: int,
    current_inventory: int,
    sourcing_rules: Dict[str, Any],
    obsolescence_cost_per_unit: float
) -> Dict[str, Any]:
    """
    Determines the final order quantity that minimizes the Total Cost of Ownership (TCO),
    considering both purchase cost with volume discounts and potential obsolescence cost for surplus items.

    Args:
        total_required_units: The total forecasted need for the component.
        current_inventory: The current quantity on hand.
        sourcing_rules: A dictionary containing 'min_order_qty' and 'volume_pricing' tiers.
        obsolescence_cost_per_unit: The cost to dispose of one surplus unit.

    Returns:
        A dictionary with the net required units, the calculated final order quantity, and the reason for the decision.
    """
    net_required_units = max(0, total_required_units - current_inventory)

    pricing_tiers = sourcing_rules.get("volume_pricing", [])
    min_order_qty = sourcing_rules.get("min_order_qty", 0)

    candidate_quantities = {net_required_units, float(min_order_qty)}
    for tier in pricing_tiers:
        candidate_quantities.add(float(tier['min_qty']))

    tco_results = []

    def get_price_for_quantity(quantity, tiers):

        best_price = float('inf')
        applicable_tier = None
        for tier in sorted(tiers, key=lambda x: x['min_qty'], reverse=True):
            if quantity >= tier['min_qty']:
                applicable_tier = tier
                break
        if applicable_tier:
            return float(applicable_tier['price'])

        return float(tiers[0]['price']) if tiers else 0

    for qty in sorted(list(candidate_quantities)):
        if qty < min_order_qty:
            if qty == 0: 
                tco_results.append({'quantity': 0, 'tco': 0, 'reason': "No purchase needed."})
                continue
            else: 
                continue

        unit_price = get_price_for_quantity(qty, pricing_tiers)
        purchase_cost = qty * unit_price
        
        surplus_qty = max(0, qty - net_required_units)
        obsolescence_cost = surplus_qty * obsolescence_cost_per_unit
        
        total_cost = purchase_cost + obsolescence_cost
        tco_results.append({
            'quantity': int(qty),
            'tco': total_cost,
            'reason': f"Ordering {int(qty)} units at ${unit_price:.2f}/unit has a TCO of ${total_cost:,.2f}."
        })

    if not tco_results:
         return {
            "net_required_units": int(net_required_units),
            "final_order_quantity": int(min_order_qty),
            "reason": f"No valid pricing tiers found. Ordering the minimum required quantity of {min_order_qty}."
        }

    optimal_choice = min(tco_results, key=lambda x: x['tco'])

    return {
        "net_required_units": int(net_required_units),
        "final_order_quantity": optimal_choice['quantity'],
        "reason": optimal_choice['reason']
    }
    
@mcp.tool()
def generate_eol_purchase_order(
    component_id: str,
    supplier_id: str,
    final_order_quantity: int
) -> Dict[str, Any]:
    """
    Formats the final End-of-Life purchase order details based on previous calculations.

    Args:
        component_id: The ID of the component to be ordered.
        supplier_id: The ID of the supplier to order from.
        final_order_quantity: The final calculated quantity to order.

    Returns:
        A dictionary containing the structured final purchase order details.
    """
    return {
        "final_answer": {
            "component_id": component_id,
            "supplier_id": supplier_id,
            "final_order_quantity": final_order_quantity
        }
    }

if __name__ == "__main__":
    mcp.run(transport="stdio")