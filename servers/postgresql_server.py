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

if __name__ == "__main__":
    mcp.run(transport="stdio")