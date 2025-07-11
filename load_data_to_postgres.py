import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql://postgres:1234@localhost:5432/mcp"
engine = create_engine(DB_URL)

def setup_database():
    table_creation_sql = """
    DROP TABLE IF EXISTS fact_trim_perf, dim_dealer, dim_trim, dim_model, dim_region CASCADE;
    CREATE TABLE dim_model (
        model_key   SERIAL PRIMARY KEY,
        model_name  VARCHAR(40) NOT NULL
    );
    CREATE TABLE dim_trim (
        trim_key   SERIAL PRIMARY KEY,
        trim_name  VARCHAR(40) NOT NULL,
        model_key  INT REFERENCES dim_model(model_key)
    );
    CREATE TABLE dim_region (
        region_key   SERIAL PRIMARY KEY,
        region_name  VARCHAR(80)
    );
    CREATE TABLE dim_dealer (
        dealer_key   SERIAL PRIMARY KEY,
        dealer_name  VARCHAR(80),
        region_key   INT REFERENCES dim_region(region_key)
    );
    CREATE TABLE fact_trim_perf (
        date                   DATE NOT NULL,
        trim_key               INT NOT NULL REFERENCES dim_trim(trim_key),
        dealer_key             INT NOT NULL REFERENCES dim_dealer(dealer_key),
        region_key             INT NOT NULL REFERENCES dim_region(region_key),
        units_sold             INT,
        revenue_amt            NUMERIC(14,2),
        variable_cost_amt      NUMERIC(14,2),
        ending_inventory_units INT,
        survey_score_sum       SMALLINT,
        survey_resp_cnt        SMALLINT,
        CONSTRAINT pk_fact_trim_perf PRIMARY KEY (date, trim_key, dealer_key)
    );
    """
    csv_to_table_map = {
        './data/dim_model.csv': 'dim_model',
        './data/dim_region.csv': 'dim_region',
        './data/dim_dealer.csv': 'dim_dealer',
        './data/dim_trim.csv': 'dim_trim',
        './data/fact_trim_perf.csv': 'fact_trim_perf'
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