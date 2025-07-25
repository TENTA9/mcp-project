import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql://postgres:1234@localhost:5432/mcp"
engine = create_engine(DB_URL)

def setup_database():
    table_creation_sql = """
    DROP TABLE IF EXISTS
        sales_incentive_map,
        internal_transfer_orders,
        purchase_order_lines,
        purchase_order_header,
        demand_forecast_log,
        quality_incidents,
        production_history,
        inventory_history,
        shipment_history,
        sales_history,
        product_substitution_rules,
        component_substitutes,
        marketing_campaigns,
        transportation_lanes,
        sourcing_rules,
        bill_of_materials,
        incentive_programs,
        chart_of_accounts,
        partners,
        production_capacity,
        locations,
        components,
        products
    CASCADE;

    CREATE TABLE products (
        product_id VARCHAR(30) PRIMARY KEY,
        product_name VARCHAR(100),
        base_model VARCHAR(50),
        trim_level VARCHAR(50),
        product_category VARCHAR(30),
        lifecycle_status VARCHAR(30),
        base_price NUMERIC(14,2),
        currency VARCHAR(10),
        standard_product_cost NUMERIC(14,2),
        end_of_service_date DATE,
        standard_production_time_hours INT
    );

    CREATE TABLE components (
        component_id VARCHAR(30) PRIMARY KEY,
        component_name VARCHAR(80),
        component_category VARCHAR(40),
        standard_cost NUMERIC(14,2),
        currency VARCHAR(10),
        unit_of_measure VARCHAR(10),
        is_critical_part BOOLEAN,
        lifecycle_status VARCHAR(30),
        shelf_life_years INT
    );

    CREATE TABLE locations (
        location_id VARCHAR(30) PRIMARY KEY,
        location_name VARCHAR(100),
        location_type VARCHAR(30),
        address VARCHAR(100),
        dock_capacity INT,
        operating_hours VARCHAR(30),
        avg_handling_hr NUMERIC(6,2),
        handling_cost_per_unit NUMERIC(10,2)
    );

    CREATE TABLE partners (
        partner_id VARCHAR(10) PRIMARY KEY,
        partner_name VARCHAR(100),
        partner_type VARCHAR(20),
        tier INT,
        address VARCHAR(100),
        quality_score INT,
        on_time_delivery_pct NUMERIC(6,2)
    );

    CREATE TABLE chart_of_accounts (
        account_id VARCHAR(30) PRIMARY KEY,
        account_name VARCHAR(50),
        parent_account_id VARCHAR(30),
        display_sequence VARCHAR(10),
        is_calculated_field BOOLEAN,
        calculation_formula VARCHAR(100),
        account_type VARCHAR(20)
    );

    CREATE TABLE incentive_programs (
        incentive_id VARCHAR(30) PRIMARY KEY,
        incentive_name VARCHAR(100),
        incentive_type VARCHAR(30),
        value_amount NUMERIC(14,2),
        value_pct NUMERIC(6,2),
        cost_to_company NUMERIC(14,2)
    );

    CREATE TABLE bill_of_materials (
        bom_line_id INT PRIMARY KEY,
        product_id VARCHAR(30),
        component_id VARCHAR(30),
        quantity_per_unit INT,
        is_critical_in_bom BOOLEAN
    );

    CREATE TABLE sourcing_rules (
        sourcing_id INT PRIMARY KEY,
        component_id VARCHAR(30),
        partner_id VARCHAR(10),
        is_primary_supplier BOOLEAN,
        volume_pricing_json TEXT,
        unit_price NUMERIC(14,2),
        currency VARCHAR(10),
        min_order_qty INT,
        lead_time_days INT,
        committed_capacity_monthly INT,
        max_capacity_monthly INT
    );

    CREATE TABLE transportation_lanes (
        lane_id VARCHAR(50) PRIMARY KEY,
        origin_loc_id VARCHAR(30),
        dest_loc_id VARCHAR(30),
        transport_mode VARCHAR(10),
        primary_carrier_id VARCHAR(10),
        distance_km INT,
        standard_transit_hr NUMERIC(5,2),
        reliability_pct NUMERIC(5,2),
        standard_cost_per_shipment NUMERIC(14,2),
        is_primary_route BOOLEAN
    );

    CREATE TABLE marketing_campaigns (
        campaign_id VARCHAR(30) PRIMARY KEY,
        campaign_name VARCHAR(100),
        target_product_id VARCHAR(30),
        target_region VARCHAR(30),
        channel VARCHAR(30),
        budget NUMERIC(14,2),
        predicted_uplift_pct NUMERIC(5,2)
    );

    CREATE TABLE component_substitutes (
        substitution_id INT PRIMARY KEY,
        original_component_id VARCHAR(30),
        substitute_component_id VARCHAR(30),
        applicable_product_id VARCHAR(30),
        interchangeability VARCHAR(20),
        substitution_priority INT,
        approval_status VARCHAR(30),
        cost_impact_pct NUMERIC(6,2)
    );

    CREATE TABLE product_substitution_rules (
        rule_id INT PRIMARY KEY,
        source_product_id VARCHAR(30),
        target_product_id VARCHAR(30),
        substitution_rate_pct NUMERIC(6,2),
        rationale VARCHAR(255)
    );

    CREATE TABLE sales_history (
        transaction_id BIGINT PRIMARY KEY,
        sales_dt DATE,
        product_id VARCHAR(30),
        location_id VARCHAR(30),
        units_sold INT,
        selling_price_per_unit NUMERIC(14,2),
        currency VARCHAR(10),
        campaign_id VARCHAR(30)
    );

    CREATE TABLE shipment_history (
        shipment_id BIGINT PRIMARY KEY,
        lane_id VARCHAR(50),
        carrier_id VARCHAR(10),
        product_id VARCHAR(30),
        quantity_shipped INT,
        shipped_ts TIMESTAMP,
        delivered_ts TIMESTAMP,
        tracking_status VARCHAR(30)
    );

    CREATE TABLE inventory_history (
        snapshot_id BIGINT PRIMARY KEY,
        snapshot_ts TIMESTAMP,
        location_id VARCHAR(30),
        item_id VARCHAR(30),
        item_type VARCHAR(20),
        quantity_on_hand INT,
        inventory_status VARCHAR(30)
    );

    CREATE TABLE production_history (
        work_order_id VARCHAR(30) PRIMARY KEY,
        production_dt DATE,
        location_id VARCHAR(30),
        product_id VARCHAR(30),
        quantity_produced INT,
        actual_takt_time_sec INT
    );

    CREATE TABLE quality_incidents (
        incident_id INT PRIMARY KEY,
        discovery_dt DATE,
        incident_source VARCHAR(30),
        location_id VARCHAR(30),
        product_id VARCHAR(30),
        component_id VARCHAR(30),
        defect_description TEXT,
        resolution_status VARCHAR(30)
    );

    CREATE TABLE demand_forecast_log (
        log_id INT PRIMARY KEY,
        log_ts TIMESTAMP,
        target_period VARCHAR(10),
        product_id VARCHAR(30),
        forecast_version INT,
        forecast_source VARCHAR(30),
        forecasted_qty INT,
        rationale VARCHAR(255)
    );

    CREATE TABLE purchase_order_header (
        po_id VARCHAR(30) PRIMARY KEY,
        partner_id VARCHAR(10),
        destination_loc_id VARCHAR(30),
        order_dt DATE,
        expected_delivery_dt DATE,
        actual_receipt_dt DATE,
        order_status VARCHAR(30),
        total_value NUMERIC(14,2),
        currency VARCHAR(10),
        payment_terms VARCHAR(30)
    );

    CREATE TABLE purchase_order_lines (
        po_line_id INT PRIMARY KEY,
        po_id VARCHAR(30),
        sourcing_id INT,
        component_id VARCHAR(30),
        quantity_ordered INT,
        quantity_received INT,
        unit_price NUMERIC(14,2),
        line_total_value NUMERIC(14,2),
        line_status VARCHAR(20),
        expected_line_delivery_dt DATE
    );

    CREATE TABLE internal_transfer_orders (
        transfer_id VARCHAR(30) PRIMARY KEY,
        requesting_loc_id VARCHAR(30),
        supplying_loc_id VARCHAR(30),
        product_id VARCHAR(30),
        quantity_requested INT,
        quantity_shipped INT,
        order_status VARCHAR(30),
        related_shipment_id BIGINT,
        created_ts TIMESTAMP
    );

    CREATE TABLE sales_incentive_map (
        map_id BIGINT PRIMARY KEY,
        transaction_id BIGINT,
        incentive_id VARCHAR(30),
        redeemed_ts TIMESTAMP
    );
    
    CREATE TABLE production_capacity (
        capacity_id BIGINT PRIMARY KEY,
        location_id VARCHAR(30),
        capacity_date DATE,
        total_capacity_hours NUMERIC(5,2),
        scheduled_hours NUMERIC(5,2),
        available_hours NUMERIC(5,2)
    );
    """

    csv_to_table_map = {
        './data/products.csv': 'products',
        './data/components.csv': 'components',
        './data/locations.csv': 'locations',
        './data/partners.csv': 'partners',
        './data/chart_of_accounts.csv': 'chart_of_accounts',
        './data/incentive_programs.csv': 'incentive_programs',
        './data/bill_of_materials.csv': 'bill_of_materials',
        './data/sourcing_rules.csv': 'sourcing_rules',
        './data/transportation_lanes.csv': 'transportation_lanes',
        './data/marketing_campaigns.csv': 'marketing_campaigns',
        './data/component_substitutes.csv': 'component_substitutes',
        './data/product_substitution_rules.csv': 'product_substitution_rules',
        './data/sales_history.csv': 'sales_history',
        './data/shipment_history.csv': 'shipment_history',
        './data/inventory_history.csv': 'inventory_history',
        './data/production_history.csv': 'production_history',
        './data/quality_incidents.csv': 'quality_incidents',
        './data/demand_forecast_log.csv': 'demand_forecast_log',
        './data/purchase_order_header.csv': 'purchase_order_header',
        './data/purchase_order_lines.csv': 'purchase_order_lines',
        './data/internal_transfer_orders.csv': 'internal_transfer_orders',
        './data/sales_incentive_map.csv': 'sales_incentive_map',
        './data/production_capacity.csv': 'production_capacity'
    }

    with engine.connect() as conn:
        print("Creating tables...")
        conn.execute(text(table_creation_sql))
        conn.commit()
        print("Tables created successfully.")

        for csv_file, table_name in csv_to_table_map.items():
            try:
                print(f"Loading {csv_file} into {table_name}...")
                df = pd.read_csv(csv_file)
                df.to_sql(table_name, conn, if_exists='append', index=False)
                print(f"-> Success.")
            except Exception as e:
                print(f"-> Error loading {csv_file}: {e}")
        conn.commit()

if __name__ == "__main__":
    setup_database()