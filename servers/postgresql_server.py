import psycopg2
import psycopg2.extras
from typing import List, Dict, Any, Optional
from mcp.server.fastmcp import FastMCP

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
def read_production_capacity(product_id: str, requested_qty: int, due_date: str) -> List[Dict[str, Any]]:
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


if __name__ == "__main__":
    mcp.run(transport="stdio")