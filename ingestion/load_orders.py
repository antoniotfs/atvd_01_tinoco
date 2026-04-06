from pathlib import Path
import pandas as pd
import duckdb

def main() -> None:
    csv_path = Path("data/raw/orders_2026_03_23.csv")
    db_path = Path("warehouse/local.duckdb")

    if not csv_path.exists():
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {csv_path}")

    df = pd.read_csv(csv_path)

    print("--- 1. Prévia dos Dados Brutos (Pandas) ---")
    print(df.head())

    conn = duckdb.connect(str(db_path))

    conn.execute("DROP TABLE IF EXISTS raw_orders")
    conn.register("orders_df", df)

    conn.execute("""
    CREATE TABLE raw_orders AS
    SELECT
        CAST(order_id AS INTEGER) AS order_id,
        CAST(customer_id AS INTEGER) AS customer_id,
        CAST(order_ts AS TIMESTAMP) AS order_ts,
        CAST(amount AS DOUBLE) AS amount,
        CAST(status AS VARCHAR) AS status
    FROM orders_df
    """)

    print("\n--- 2. Tabela 'raw_orders' criada com sucesso no DuckDB! ---")

    result = conn.execute("""
    SELECT
        status,
        COUNT(*) AS total_orders,
        ROUND(SUM(amount), 2) AS total_amount
    FROM raw_orders
    GROUP BY status
    ORDER BY total_amount DESC
    """).fetchdf()

    print("\n--- 3. Resultado Final (Agregado por Status) ---")
    print(result)

    conn.close()

if __name__ == "__main__":
    main()