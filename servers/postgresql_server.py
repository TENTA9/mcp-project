import json
import psycopg2
import psycopg2.extras
from typing import List, Dict, Any, Optional
from mcp.server.fastmcp import FastMCP
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import re
from decimal import Decimal

mcp = FastMCP("PostgreSQLServer", log_level="INFO")

DB_PARAMS = {
    "dbname": "mcp",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
}

class PostgresConnection:
    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.conn = None

    def __enter__(self):
        self.conn = psycopg2.connect(**self.db_params)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

@mcp.tool()
def calculate_sales_history(product_id: Optional[str] = None, location_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Calculates sales statistics (mean and standard deviation) from 'Sales History' data. 
    Can be filtered by product_id and/or location_id.

    Args:
        product_id (Optional[str]): The unique identifier for the product to filter by.
        location_id (Optional[str]): The unique identifier for the sales location to filter by.
    
    Returns:
        List[Dict[str, Any]]: A list containing a single dictionary with the calculated 'mean' and 
                             'standard_deviation' for the filtered sales data.
    """
    query = """
        SELECT 
            AVG(units_sold) AS mean,
            COALESCE(STDDEV(units_sold), 0) AS standard_deviation
        FROM 
            Sales_History
        WHERE 1=1
    """
    params = []
    if product_id:
        query += " AND product_id = %s"
        params.append(product_id)
        
    if location_id:
        query += " AND location_id = %s"
        params.append(location_id)
        
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, tuple(params))
            return cur.fetchall()

@mcp.tool()
def read_inventory_history(item_id: str, location_id: str) -> List[Dict[str, Any]]:
    """
    Reads the most recent inventory quantity for a specific item at a specific location.

    Args:
        item_id (str): The ID of the product or component to look up (e.g., 'MODEL-C-EV', 'P-404-BAT-LG').
        location_id (str): The ID of the location where the inventory is located (e.g., 'P1_ULSAN', 'DEALER_SEOUL').
        
    Returns:
        List[Dict[str, Any]]: A list containing a single dictionary with the latest 'quantity_on_hand'. 
                             Returns an empty list if no record is found.
    """
    query = """
        SELECT 
            quantity_on_hand 
        FROM 
            Inventory_History
        WHERE 
            item_id = %s AND location_id = %s
        ORDER BY 
            snapshot_ts DESC
        LIMIT 1;
    """
    params = (item_id, location_id)
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()
        
@mcp.tool()
def read_products(product_id: str) -> List[Dict[str, Any]]:
    """
    Retrieves detailed master data for a specific product using its product_id.
    
    Args:
        product_id (str): The unique ID (SKU) of the product to retrieve.
        
    Returns:
        A list containing a single dictionary with the product's full details. 
        The dictionary includes the following keys:
        - product_id
        - product_name
        - base_model
        - trim_level
        - product_category
        - lifecycle_status
        - base_price
        - currency
        - standard_product_cost
        - end_of_service_date
        - standard_production_time_hours
    """
    query = "SELECT * FROM Products WHERE product_id = %s;"
    params = (product_id,)
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()

@mcp.tool()
def evaluate_production_capacity(product_id: str, requested_qty: int, due_date: str) -> List[Dict[str, Any]]:
    """
    Checks if there is enough production capacity to produce a requested quantity of a product by a specific due date.
    It calculates the required hours and compares it with the total available hours from today until the due date.
    
    Args:
        product_id: The ID of the product to be produced.
        requested_qty: The requested quantity for production.
        due_date: The requested completion date in 'YYYY-MM-DD' format.
        
    Returns:
        A list containing a single dictionary with 'is_capacity_available', 'total_available_hours', and 'required_hours'.
    """
    query = """
        WITH product_time AS (
            -- 1. Products 테이블에서 제품 1개당 표준 생산 시간을 가져옵니다.
            SELECT standard_production_time_hours 
            FROM Products 
            WHERE product_id = %s
        ), available_capacity AS (
            -- 2. Production_Capacity 테이블에서 오늘부터 마감일까지의 총 가용 시간을 합산합니다.
            SELECT SUM(available_hours) as total_available_hours
            FROM Production_Capacity
            WHERE capacity_date BETWEEN CURRENT_DATE AND %s
        )
        SELECT 
            -- 3. 총 필요 시간과 총 가용 시간을 계산하고, 가능 여부를 비교합니다.
            pt.standard_production_time_hours * %s AS required_hours,
            ac.total_available_hours,
            (ac.total_available_hours >= pt.standard_production_time_hours * %s) AS is_capacity_available
        FROM 
            product_time pt, available_capacity ac;
    """
    params = (product_id, due_date, requested_qty, requested_qty)
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()

@mcp.tool()        
def read_bill_of_materials(product_id: str) -> List[Dict[str, Any]]:
    """
    Retrieves the entire Bill of Materials (BOM) for a specific product_id, listing all components and their quantities needed.
    
    Args:
        product_id (str): The unique ID of the final product for which to retrieve the BOM.
        
    Returns:
        A list of dictionaries, where each dictionary represents a component line in the BOM. 
        The dictionary includes the following keys:
        - bom_line_id
        - product_id
        - component_id
        - quantity_per_unit
        - is_critical_in_bom
    """
    query = "SELECT * FROM Bill_of_Materials WHERE product_id = %s;"
    params = (product_id,)
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()      
        
@mcp.tool()
def read_inventory_history_by_components(component_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Retrieves the entire inventory history (all snapshots across all dates) for a given list of component IDs.
    
    Args:
        component_ids (List[str]): A list of component IDs to search for (e.g., ['P-404-BAT-LG', 'P-505-CHIP']).
                                   To search for a single component, it must be provided in a list.
        
    Returns:
        A list of dictionaries, where each dictionary is a historical inventory record. 
        Each dictionary includes the following keys:
        - snapshot_id
        - snapshot_ts
        - location_id
        - item_id
        - item_type
        - quantity_on_hand
        - inventory_status
    """
    query = """
        SELECT 
            * FROM 
            Inventory_History
        WHERE 
            item_id = ANY(%s);
    """
    params = (component_ids,)
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()
        
@mcp.tool()
def read_purchase_order_lines(component_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Retrieves open purchase order lines for a given list of component IDs.
    This is used to check for upcoming, unreceived deliveries.
    
    Args:
        component_ids: A list of component IDs to search for (e.g., ['P-404-BAT-LG', 'P-505-CHIP']).
                       To search for a single component, provide a list with one item.
        
    Returns:
        A list of dictionaries, where each dictionary is an open purchase order line.
        Each dictionary includes the following keys:
        - po_line_id
        - po_id
        - sourcing_id
        - component_id
        - quantity_ordered
        - quantity_received
        - unit_price
        - line_total_value
        - line_status
        - expected_line_delivery_dt
    """
    query = """
        SELECT 
            * FROM 
            Purchase_Order_Lines
        WHERE 
            component_id = ANY(%s) AND line_status = 'OPEN';
    """
    params = (component_ids,)
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()
        
@mcp.tool()
def read_sourcing_rules(shortages: List[str]) -> List[Dict[str, Any]]:
    """
    Retrieves sourcing rules for a given list of component IDs.
    This tool provides information about suppliers, pricing, lead times, and capacity for specific components.

    Args:
        shortages (List[str]): A list of component_ids for which to find sourcing rules.
        
    Returns:
        A list of dictionaries containing the sourcing rules for the specified components.
        Each dictionary includes the following keys:
        - sourcing_id
        - component_id
        - partner_id
        - is_primary_supplier
        - volume_pricing_json
        - unit_price
        - currency
        - min_order_qty
        - lead_time_days
        - committed_capacity_monthly
        - max_capacity_monthly
    """
    query = """
        SELECT 
            * FROM 
            Sourcing_Rules
        WHERE 
            component_id = ANY(%s);
    """
    params = (shortages,)
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()
        
@mcp.tool()
def read_marketing_campaigns(product_id: str, upcoming_campaigns: List[str], baseline_forecast_mc: int) -> List[Dict[str, Any]]:
    """
    Calculates the expected sales uplift quantity from specified marketing campaigns.

    Args:
        product_id: The ID of the product to forecast.
        upcoming_campaigns: A list of campaign names active in the target period.
        baseline_forecast_mc: The baseline demand forecast before applying campaign effects.
        
    Returns:
        A list containing a single dictionary with the calculated 'campaign_uplift_qty'.
    """
    query = """
        SELECT 
            COALESCE(SUM(predicted_uplift_pct), 0) as total_uplift_pct
        FROM 
            Marketing_Campaigns
        WHERE 
            target_product_id = %s AND campaign_name = ANY(%s);
    """
    params = (product_id, upcoming_campaigns)
    
    total_uplift_pct = 0
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            result = cur.fetchone()
            if result:
                total_uplift_pct = result['total_uplift_pct']

    campaign_uplift_quantity = baseline_forecast_mc * (total_uplift_pct / 100)
    
    return [{"campaign_uplift_qty": int(campaign_uplift_quantity)}]

@mcp.tool()
def retrieve_primary_partners(component_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Retrieves the primary partner (supplier) for a given list of component IDs from the Sourcing_Rules table.

    Args:
        component_ids: A list of component IDs to find the primary suppliers for.
        
    Returns:
        A list of dictionaries, each containing the component_id and its corresponding primary partner_id.
    """
    query = """
        SELECT
            component_id,
            partner_id
        FROM
            Sourcing_Rules
        WHERE
            component_id = ANY(%s) AND is_primary_supplier = TRUE;
    """
    params = (component_ids,)
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()

@mcp.tool()
def search_alternative_suppliers(component_id: str, primary_supplier_id: str) -> List[Dict[str, Any]]:
    """
    Searches for all alternative suppliers for a given component, excluding the primary supplier.
    It joins Sourcing_Rules and Partners tables to provide comprehensive details for each option.

    Args:
        component_id: The ID of the component that has a shortage.
        primary_supplier_id: The ID of the primary supplier to be excluded from the search results.

    Returns:
        A list of dictionaries, where each dictionary is a potential mitigation option 
        representing an alternative supplier with their terms.
    """
    query = """
        SELECT
            'ALTERNATIVE_SUPPLIER' AS type,
            sr.component_id,
            sr.partner_id AS supplier_id,
            p.partner_name AS supplier_name,
            sr.lead_time_days,
            sr.unit_price,
            p.quality_score
        FROM
            Sourcing_Rules sr
        JOIN
            Partners p ON sr.partner_id = p.partner_id
        WHERE
            sr.component_id = %s
            AND sr.partner_id != %s;
    """
    params = (component_id, primary_supplier_id)
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            mitigation_options = cur.fetchall()
            return {"mitigation_options": mitigation_options}

@mcp.tool()
def model_transportation_routes(
    source_plant_id: str, 
    new_hub_id: str, 
    target_dealer_id: str
) -> Dict[str, Any]:
    """
    Models and compares two transportation routes: a direct route and a new route via a hub.
    It retrieves cost and time for each segment from the Transportation_Lanes and Locations tables.

    Args:
        source_plant_id: The starting location ID (typically a plant).
        new_hub_id: The ID of the hub to be considered as a waypoint.
        target_dealer_id: The final destination ID (typically a dealer).

    Returns:
        A dictionary containing two models: 'current_route_model' for the direct path, 
        and 'new_route_model' for the path through the hub.
    """
    
    current_route_model = {}
    new_route_model = {}

    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT lane_id, standard_transit_hr, standard_cost_per_shipment FROM Transportation_Lanes WHERE origin_loc_id = %s AND dest_loc_id = %s",
                (source_plant_id, target_dealer_id)
            )
            direct_route = cur.fetchone()
            if direct_route:
                current_route_model = {
                    "lane_id": direct_route['lane_id'],
                    "total_transit_hr": float(direct_route['standard_transit_hr']),
                    "direct_cost": float(direct_route['standard_cost_per_shipment'])
                }

            cur.execute(
                "SELECT lane_id, standard_transit_hr, standard_cost_per_shipment FROM Transportation_Lanes WHERE origin_loc_id = %s AND dest_loc_id = %s",
                (source_plant_id, new_hub_id)
            )
            leg1 = cur.fetchone()

            cur.execute(
                "SELECT location_id, avg_handling_hr, handling_cost_per_unit FROM Locations WHERE location_id = %s",
                (new_hub_id,)
            )
            hub = cur.fetchone()

            cur.execute(
                "SELECT lane_id, standard_transit_hr, standard_cost_per_shipment FROM Transportation_Lanes WHERE origin_loc_id = %s AND dest_loc_id = %s",
                (new_hub_id, target_dealer_id)
            )
            leg2 = cur.fetchone()

            new_route_model = {
                "leg1": {
                    "lane_id": leg1['lane_id'] if leg1 else None,
                    "transit_hr": float(leg1['standard_transit_hr']) if leg1 else 0,
                    "cost": float(leg1['standard_cost_per_shipment']) if leg1 else 0
                },
                "hub": {
                    "location_id": hub['location_id'] if hub else new_hub_id,
                    "handling_hr": float(hub['avg_handling_hr']) if hub else 0,
                    "handling_cost": float(hub['handling_cost_per_unit']) if hub else 0
                },
                "leg2": {
                    "lane_id": leg2['lane_id'] if leg2 else None,
                    "transit_hr": float(leg2['standard_transit_hr']) if leg2 else 0,
                    "cost": float(leg2['standard_cost_per_shipment']) if leg2 else 0
                }
            }

    return {
        "current_route_model": current_route_model,
        "new_route_model": new_route_model
    }
    
@mcp.tool()
def aggregate_trim_performance(
    base_model: str, 
    period: str
) -> Dict[str, Any]:
    """
    Aggregates historical performance data for all trims of a target base_model for a given period.
    It calculates the unit margin for each trim by joining Products and Sales_History tables.

    Args:
        base_model: The base model of the product lineup to be analyzed (e.g., 'MODEL-B').
        period: The target period for the analysis (e.g., '1 quarter', '1 year').

    Returns:
        A dictionary containing a 'trim_performance_data' list.
    """
    today = date.today()
    end_date = today
    start_date = today - relativedelta(years=1) 

    try:
        parts = period.split()
        if len(parts) == 2 and parts[0].isdigit():
            value = int(parts[0])
            unit = parts[1].lower()
            if 'quarter' in unit:
                start_date = today - relativedelta(months=3 * value)
            elif 'year' in unit:
                start_date = today - relativedelta(years=value)
            elif 'month' in unit:
                start_date = today - relativedelta(months=value)
            elif 'week' in unit:
                start_date = today - relativedelta(weeks=value)
            elif 'day' in unit:
                start_date = today - relativedelta(days=value)
    except Exception:
        pass

    query = """
        SELECT
            p.product_id,
            p.standard_product_cost,
            p.standard_production_time_hours,
            COALESCE(AVG(sh.selling_price_per_unit), 0) - p.standard_product_cost AS unit_margin
        FROM
            Products p
        LEFT JOIN
            Sales_History sh ON p.product_id = sh.product_id AND sh.sales_dt BETWEEN %s AND %s
        WHERE
            p.base_model = %s
        GROUP BY
            p.product_id, p.standard_product_cost, p.standard_production_time_hours
        ORDER BY
            p.product_id;
    """
    params = (start_date, end_date, base_model)
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            performance_data = cur.fetchall()
            return {"trim_performance_data": performance_data}
        
@mcp.tool()
def calculate_optimal_shift(
    least_efficient_trim: Dict[str, Any], 
    most_efficient_trim: Dict[str, Any],
    period: str = "1 quarter"
) -> Dict[str, Any]:
    """
    Calculates the optimal production shift from a low-efficiency trim to a high-efficiency trim,
    considering market demand and production capacity constraints.

    Args:
        least_efficient_trim: A dictionary with details of the trim to reduce production for.
        most_efficient_trim: A dictionary with details of the trim to reallocate production to.
        period: The future period for which to calculate the shift (e.g., '1 quarter', '1 month').

    Returns:
        A dictionary containing the optimal simulation parameters including what to reduce, 
        what to reallocate to, and the binding constraint.
    """
    most_efficient_id = most_efficient_trim['product_id']
    most_efficient_time = most_efficient_trim['production_time']
    least_efficient_id = least_efficient_trim['product_id']
    least_efficient_time = least_efficient_trim['production_time']

    today = date.today()
    start_date = today
    
    parts = period.split()
    if len(parts) == 2 and parts[0].isdigit():
        value = int(parts[0])
        unit = parts[1].lower()
        if 'quarter' in unit:
            end_date = today + relativedelta(months=3 * value)
        elif 'year' in unit:
            end_date = today + relativedelta(years=value)
        elif 'month' in unit:
            end_date = today + relativedelta(months=value)
        else:
            raise ValueError(f"Unknown time unit in period: '{parts[1]}'")
    else:
        raise ValueError(f"Invalid period format: '{period}'. Expected format like '1 quarter' or '2 years'.")

    market_constraint_qty = 0
    production_constraint_hours = 0
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            start_period_str = start_date.strftime('%Y-%m')
            end_period_str = end_date.strftime('%Y-%m')

            cur.execute(
                "SELECT COALESCE(SUM(forecasted_qty), 0) as total_forecast FROM Demand_Forecast_Log WHERE product_id = %s AND forecast_source = 'FINAL_PLAN' AND target_period BETWEEN %s AND %s",
                (most_efficient_id, start_period_str, end_period_str)
            )
            result = cur.fetchone()
            if result:
                market_constraint_qty = result['total_forecast']

            cur.execute(
                "SELECT COALESCE(SUM(available_hours), 0) as total_available FROM Production_Capacity WHERE capacity_date BETWEEN %s AND %s",
                (start_date, end_date)
            )
            result = cur.fetchone()
            if result:
                production_constraint_hours = result['total_available']

    max_producible_qty = production_constraint_hours / most_efficient_time if most_efficient_time > 0 else 0
    reallocate_qty = min(market_constraint_qty, max_producible_qty)
    binding_constraint = "Market Demand Forecast" if market_constraint_qty < max_producible_qty else "Production Capacity"

    hours_to_free_up = reallocate_qty * most_efficient_time
    reduce_qty = hours_to_free_up / least_efficient_time if least_efficient_time > 0 else 0

    result = {
        "optimal_simulation_parameters": {
            "reduce_target": {
                "product_id": least_efficient_id,
                "quantity": int(reduce_qty)
            },
            "reallocate_to": {
                "product_id": most_efficient_id,
                "quantity": int(reallocate_qty)
            },
            "binding_constraint": binding_constraint
        }
    }
    return result

@mcp.tool()
def calculate_lifetime_demand(
    affected_products: List[Dict[str, Any]],
    component_id: str
) -> Dict[str, Any]:
    """
    Calculates the total lifetime demand for a discontinued component, including future production and service needs.

    Args:
        affected_products: A list of products affected by the component's discontinuation. 
                           Each dict must have 'product_id' and 'end_of_service_date'.
        component_id: The ID of the component being discontinued.

    Returns:
        A dictionary containing the calculated 'production_demand', 'service_demand', and 'total_required_units'.
    """
    if not affected_products:
        return {
            "total_lifetime_demand": {
                "production_demand": 0,
                "service_demand": 0,
                "total_required_units": 0
            }
        }

    product_ids = [p['product_id'] for p in affected_products]
    
    production_demand = 0
    service_demand = 0
    total_future_forecast = 0

    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            start_period = date.today().strftime('%Y-%m')
            cur.execute(
                "SELECT product_id, COALESCE(SUM(forecasted_qty), 0) as total_forecast FROM Demand_Forecast_Log WHERE product_id = ANY(%s) AND forecast_source = 'FINAL_PLAN' AND target_period >= %s GROUP BY product_id",
                (product_ids, start_period)
            )
            forecasts = {row['product_id']: row['total_forecast'] for row in cur.fetchall()}
            total_future_forecast = sum(forecasts.values())

            cur.execute(
                "SELECT product_id, quantity_per_unit FROM Bill_of_Materials WHERE component_id = %s AND product_id = ANY(%s)",
                (component_id, product_ids)
            )
            bom_map = {row['product_id']: row['quantity_per_unit'] for row in cur.fetchall()}

            for pid in product_ids:
                production_demand += forecasts.get(pid, 0) * bom_map.get(pid, 0)

            cur.execute(
                "SELECT COUNT(*) as total_incidents FROM Quality_Incidents WHERE component_id = %s AND product_id = ANY(%s)",
                (component_id, product_ids)
            )
            total_incidents = cur.fetchone()['total_incidents']

            cur.execute(
                "SELECT COALESCE(SUM(units_sold), 0) as total_sales FROM Sales_History WHERE product_id = ANY(%s)",
                (product_ids,)
            )
            total_sales = cur.fetchone()['total_sales']

            if total_sales > 0:
                failure_rate = Decimal(total_incidents) / Decimal(total_sales)
                service_demand = failure_rate * Decimal(total_future_forecast)
            else:
                service_demand = 0

    total_required_units = production_demand + service_demand
    
    return {
        "total_lifetime_demand": {
            "production_demand": int(production_demand),
            "service_demand": int(service_demand),
            "total_required_units": int(total_required_units)
        }
    }
    
@mcp.tool()
def get_component_sourcing_data(component_id: str) -> Dict[str, Any]:
    """
    Collects all necessary cost and sourcing data for an EOL (End-of-Life) buy calculation for a specific component.

    Args:
        component_id: The ID of the component to gather data for.

    Returns:
        A dictionary containing current total inventory, sourcing rules (MOQ, volume pricing),
        and the obsolescence cost per unit for the specified component.
    """
    current_inventory = 0
    sourcing_rules = {}
    obsolescence_cost_per_unit = 0

    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(quantity_on_hand), 0) as total_inventory
                FROM Inventory_History
                WHERE item_id = %s
                  AND snapshot_ts = (SELECT MAX(snapshot_ts) FROM Inventory_History WHERE item_id = %s);
                """,
                (component_id, component_id)
            )
            inv_result = cur.fetchone()
            if inv_result:
                current_inventory = inv_result['total_inventory']

            cur.execute(
                "SELECT min_order_qty, volume_pricing_json FROM Sourcing_Rules WHERE component_id = %s AND is_primary_supplier = TRUE",
                (component_id,)
            )
            sourcing_result = cur.fetchone()
            if sourcing_result:
                sourcing_rules = {
                    "min_order_qty": sourcing_result['min_order_qty'],
                    "volume_pricing": json.loads(sourcing_result['volume_pricing_json']).get('tiers', [])
                }

            cur.execute(
                "SELECT standard_cost FROM Components WHERE component_id = %s",
                (component_id,)
            )
            cost_result = cur.fetchone()
            if cost_result:
                obsolescence_cost_per_unit = cost_result['standard_cost']

    return {
        "current_inventory": int(current_inventory),
        "sourcing_rules": sourcing_rules,
        "obsolescence_cost_per_unit": float(obsolescence_cost_per_unit)
    }

if __name__ == "__main__":
    mcp.run(transport="stdio")