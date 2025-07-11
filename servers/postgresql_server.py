import psycopg2
import psycopg2.extras
from typing import List, Dict, Any, Optional
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("PostgreSQL Predefined Tools Server", log_level="INFO")

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
def search_trims_by_name(search_term: str) -> List[Dict[str, Any]]:
    """
    Search for trim information by a text-based name. Used to find the trim_key for a given trim name.
    
    Args:
        search_term: The name or partial name of the trim to search for (e.g., 'Sorento', 'K5 Hybrid').
        
    Returns:
        A list of matching trims with their trim_key and trim_name.
    """
    query = "SELECT trim_key, trim_name, model_key FROM dim_trim WHERE trim_name ILIKE %s;"
    params = (f'%{search_term}%',)
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()

@mcp.tool()
def get_sales_velocity(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Calculate the average number of units sold per dealer per month."""
    query = """
        SELECT
            d.dealer_name,
            date_trunc('month', f.date)::date AS sales_month,
            AVG(f.units_sold) AS average_monthly_sales
        FROM fact_trim_perf f
        JOIN dim_dealer d ON f.dealer_key = d.dealer_key
        WHERE f.date BETWEEN %s AND %s
        GROUP BY d.dealer_name, sales_month
        ORDER BY d.dealer_name, sales_month;
    """
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (start_date, end_date))
            return cur.fetchall()

@mcp.tool()
def get_cmu(trim_key: int) -> List[Dict[str, Any]]:
    """Calculate the contribution margin per unit (CMU) for a specific trim."""
    query = """
        SELECT (SUM(revenue_amt - variable_cost_amt) / NULLIF(SUM(units_sold), 0)) AS cmu
        FROM fact_trim_perf WHERE trim_key = %s;
    """
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (trim_key,))
            return cur.fetchall()

@mcp.tool()
def get_idoh(trim_key: int, as_of_date: str) -> List[Dict[str, Any]]:
    """Calculate the inventory days of hold (IDOH) based on the last 30 days sales rate."""
    query = """
        WITH latest_inventory AS (
            SELECT COALESCE(ending_inventory_units, 0) as inventory_units
            FROM fact_trim_perf WHERE trim_key = %s AND date <= %s ORDER BY date DESC LIMIT 1
        ), sales_rate AS (
            SELECT AVG(units_sold) AS avg_daily_sales
            FROM fact_trim_perf WHERE trim_key = %s AND date BETWEEN (%s::date - interval '30 days') AND %s
        )
        SELECT li.inventory_units / NULLIF(sr.avg_daily_sales, 0) AS idoh
        FROM latest_inventory li, sales_rate sr;
    """
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (trim_key, as_of_date, trim_key, as_of_date, as_of_date))
            return cur.fetchall()

@mcp.tool()
def get_attach_rate(trim_key: int) -> List[Dict[str, Any]]:
    """Calculate the attach rate of a trim model, defined as its sales percentage within its parent model."""
    query = """
        WITH trim_sales AS (
            SELECT SUM(units_sold) as total_units FROM fact_trim_perf WHERE trim_key = %s
        ), model_sales AS (
            SELECT SUM(f.units_sold) as total_units
            FROM fact_trim_perf f
            WHERE f.trim_key IN (SELECT t2.trim_key FROM dim_trim t2 WHERE t2.model_key = (SELECT t1.model_key FROM dim_trim t1 WHERE t1.trim_key = %s))
        )
        SELECT (ts.total_units / NULLIF(ms.total_units, 0)) * 100 AS attach_rate_percentage
        FROM trim_sales ts, model_sales ms;
    """
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (trim_key, trim_key))
            return cur.fetchall()

@mcp.tool()
def get_css(trim_key: int) -> List[Dict[str, Any]]:
    """
    Calculates the customer satisfaction score (CSS) for a specific trim. This tool requires a trim_key.

    Args:
        trim_key: The primary key of the trim for which to calculate the CSS.
    """
    query = "SELECT SUM(survey_score_sum) / NULLIF(SUM(survey_resp_cnt), 0) AS css FROM fact_trim_perf WHERE trim_key = %s;"
    params = (trim_key,)
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()

@mcp.tool()
def get_market_share(trim_key: int, segment_total_sales: int) -> List[Dict[str, Any]]:
    """Calculate the market share of our company’s trim model against the total segment sales volume."""
    if segment_total_sales <= 0:
        raise ValueError("Total segment sales must be a positive number.")
    query = "SELECT (SUM(units_sold)::float / %s) * 100 AS market_share_percentage FROM fact_trim_perf WHERE trim_key = %s;"
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (segment_total_sales, trim_key))
            return cur.fetchall()

@mcp.tool()
def calculate_pmt(target_profit_margin: float, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Calculate the profit maintenance threshold (PMT) based on total revenue and a target profit margin."""
    if not 0 < target_profit_margin < 1:
        raise ValueError("Target profit margin must be between 0 and 1 (e.g., 0.15 for 15%).")
    query = "SELECT SUM(revenue_amt) * %s AS profit_maintenance_threshold FROM fact_trim_perf WHERE date BETWEEN %s AND %s;"
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (target_profit_margin, start_date, end_date))
            return cur.fetchall()

@mcp.tool()
def calculate_uvp(trim_key: Optional[int] = None) -> List[Dict[str, Any]]:
    """Calculate the unique value proposition (UVP) score from user reviews. This is an alias for customer satisfaction score."""
    if trim_key:
        query = "SELECT SUM(survey_score_sum) / NULLIF(SUM(survey_resp_cnt), 0) AS uvp_score FROM fact_trim_perf WHERE trim_key = %s;"
        params = (trim_key,)
    else:
        query = "SELECT SUM(survey_score_sum) / NULLIF(SUM(survey_resp_cnt), 0) AS uvp_score FROM fact_trim_perf;"
        params = None
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()

@mcp.tool()
def get_total_assets() -> List[Dict[str, Any]]:
    """Get total assets from the database, defined as the monetary value of the total ending inventory."""
    query = """
        WITH inventory_cost AS (
            SELECT SUM(variable_cost_amt) / NULLIF(SUM(units_sold), 0) as avg_cost_per_unit
            FROM fact_trim_perf
        )
        SELECT SUM(f.ending_inventory_units) * ic.avg_cost_per_unit AS total_assets_value
        FROM fact_trim_perf f, inventory_cost ic
        GROUP BY ic.avg_cost_per_unit;
    """
    with PostgresConnection(DB_PARAMS) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            return cur.fetchall()

if __name__ == "__main__":
    mcp.run(transport="stdio")