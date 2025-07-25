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
def calculate_sales_history(product_id: str, location_id: str) -> List[Dict[str, Any]]:
    """
    Calculates the mean and standard deviation of units sold for a specific product at a specific location from the sales history.
    """
    query = """
        SELECT 
            AVG(units_sold) AS mean,
            COALESCE(STDDEV(units_sold), 0) AS standard_deviation
        FROM 
            Sales_History
        WHERE 
            product_id = %s AND location_id = %s;
    """
    params = (product_id, location_id)
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()

@mcp.tool()
def read_inventory_history(item_id: str, location_id: str) -> List[Dict[str, Any]]:
    """
    Reads the most recent inventory quantity for a specific item at a specific location.
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
    Retrieves all information for a specific product using its product_id.
    
    Args:
        product_id: The unique ID of the product to retrieve.
        
    Returns:
        A list containing a single dictionary with the product's full details.
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
    Retrieves the entire Bill of Materials (BOM) for a specific product_id.
    This lists all components and their quantities needed to build the product.
    
    Args:
        product_id: The unique ID of the final product.
        
    Returns:
        A list of dictionaries, where each dictionary represents a component line in the BOM.
    """
    query = "SELECT * FROM Bill_of_Materials WHERE product_id = %s;"
    params = (product_id,)
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()      
        
@mcp.tool()
def read_inventory_history(component_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Retrieves the entire inventory history for a given list of component IDs.
    
    Args:
        component_ids: A list of component IDs to search for (e.g., ['P-404-BAT-LG', 'P-505-CHIP']).
                       To search for a single component, provide a list with one item.
        
    Returns:
        A list of dictionaries, where each dictionary is a row from the inventory history 
        matching any of the provided component IDs.
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
        A list of dictionaries, where each dictionary is an open purchase order line
        matching any of the provided component IDs.
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

    Args:
        shortages: A list of component_ids for which to find sourcing rules.
        
    Returns:
        A list of dictionaries containing the sourcing rules for the specified components.
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


if __name__ == "__main__":
    mcp.run(transport="stdio")