import os
import sys

# Add virtualenv bin/Scripts directory to PATH so that dagster-dbt can find the dbt executable
dbt_project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dbt_project"))
venv_scripts = os.path.abspath(os.path.join(dbt_project_dir, "..", ".venv", "Scripts"))
venv_bin = os.path.abspath(os.path.join(dbt_project_dir, "..", ".venv", "bin"))

path_entries = os.environ.get("PATH", "").split(os.pathsep)
if venv_scripts not in path_entries:
    path_entries.insert(0, venv_scripts)
if venv_bin not in path_entries:
    path_entries.insert(0, venv_bin)
os.environ["PATH"] = os.pathsep.join(path_entries)

from dagster import asset, Output, AssetExecutionContext, AssetKey, Failure
from dagster_dbt import dbt_assets, DagsterDbtTranslator, DbtProject, DbtCliResource
import subprocess
import pandas as pd
import duckdb

# dbt project setup
dbt_project = DbtProject(project_dir=dbt_project_dir)
dbt_project.prepare_if_dev()

@asset(compute_kind="python")
def bronze_raw_data(context: AssetExecutionContext):
    """Generates mock customer and transaction data and saves them as CSV files."""
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate customers
    cust_df = pd.DataFrame([
        {"id": 101, "name": "Alice Smith", "email": "alice@example.com", "created_at": "2026-01-15 10:00:00", "status": "active"},
        {"id": 102, "name": "Bob Jones", "email": "bob@example.com", "created_at": "2026-02-10 11:30:00", "status": "active"},
        {"id": 103, "name": "Charlie Brown", "email": "charlie@example.com", "created_at": "2026-03-05 09:15:00", "status": "inactive"},
        {"id": 104, "name": "Diana Prince", "email": "diana@example.com", "created_at": "2026-04-20 14:00:00", "status": "active"},
        {"id": 105, "name": "Evan Wright", "email": "evan@example.com", "created_at": "2026-05-12 16:45:00", "status": None}
    ])
    
    # Generate sales
    sales_df = pd.DataFrame([
        {"sale_id": 1, "customer_id": 101, "product": "Premium Subscription", "amount": 99.99, "status": "COMPLETED", "sale_date": "2026-06-01 12:00:00"},
        {"sale_id": 2, "customer_id": 102, "product": "Mechanical Keyboard", "amount": 149.50, "status": "COMPLETED", "sale_date": "2026-06-02 14:30:00"},
        {"sale_id": 3, "customer_id": 101, "product": "USB-C Hub", "amount": 45.00, "status": "PENDING", "sale_date": "2026-06-03 10:15:00"},
        {"sale_id": 4, "customer_id": 104, "product": "Ergonomic Office Chair", "amount": 349.99, "status": "COMPLETED", "sale_date": "2026-06-05 16:00:00"},
        {"sale_id": 5, "customer_id": 103, "product": "Gaming Mouse", "amount": 79.99, "status": "CANCELLED", "sale_date": "2026-06-06 09:00:00"},
        {"sale_id": 6, "customer_id": 105, "product": "UltraWide Monitor 4K", "amount": 899.00, "status": "COMPLETED", "sale_date": "2026-06-07 11:00:00"}
    ])
    
    cust_path = os.path.join(data_dir, "raw_customers.csv")
    sales_path = os.path.join(data_dir, "raw_sales.csv")
    
    cust_df.to_csv(cust_path, index=False)
    sales_df.to_csv(sales_path, index=False)
    
    context.log.info(f"Mock data generated: {len(cust_df)} customers, {len(sales_df)} sales.")
    return Output(
        value=(cust_path, sales_path),
        metadata={
            "customer_count": len(cust_df),
            "sales_count": len(sales_df)
        }
    )

@asset(key_prefix="raw", deps=[bronze_raw_data], compute_kind="duckdb")
def bronze_customers(context: AssetExecutionContext):
    """Loads CSV files into DuckDB bronze customers table."""
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    db_path = os.path.join(data_dir, "analytics.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("DROP TABLE IF EXISTS bronze_customers;")
        conn.execute(f"CREATE TABLE bronze_customers AS SELECT * FROM read_csv_auto('{os.path.join(data_dir, 'raw_customers.csv')}');")
        cust_count = conn.execute("SELECT count() FROM bronze_customers;").fetchone()[0]
        context.log.info(f"Loaded {cust_count} customers to DuckDB.")
        return Output(value=db_path, metadata={"rows": cust_count})
    finally:
        conn.close()

@asset(key_prefix="raw", deps=[bronze_raw_data], compute_kind="duckdb")
def bronze_sales(context: AssetExecutionContext):
    """Loads CSV files into DuckDB bronze sales table."""
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    db_path = os.path.join(data_dir, "analytics.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("DROP TABLE IF EXISTS bronze_sales;")
        conn.execute(f"CREATE TABLE bronze_sales AS SELECT * FROM read_csv_auto('{os.path.join(data_dir, 'raw_sales.csv')}');")
        sales_count = conn.execute("SELECT count() FROM bronze_sales;").fetchone()[0]
        context.log.info(f"Loaded {sales_count} sales to DuckDB.")
        return Output(value=db_path, metadata={"rows": sales_count})
    finally:
        conn.close()

@asset(deps=[AssetKey(["raw", "bronze_customers"]), AssetKey(["raw", "bronze_sales"])], compute_kind="soda")
def soda_validate_bronze(context: AssetExecutionContext):
    """Runs Soda Core checks on the raw bronze tables."""
    soda_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "soda"))
    config_path = os.path.join(soda_dir, "configuration.yml")
    checks_path = os.path.join(soda_dir, "checks_bronze.yml")
    
    # Under virtual env, soda.exe is located inside .venv/Scripts/
    venv_bin_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".venv", "Scripts"))
    soda_exe = os.path.join(venv_bin_dir, "soda.exe")
    cmd = [soda_exe if os.path.exists(soda_exe) else "soda", "scan", "-d", "dev", "-c", config_path, checks_path]

    context.log.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    context.log.info(result.stdout)
    
    if result.returncode != 0:
        raise Failure(
            description=f"Bronze quality gate rejected the load (Soda exit {result.returncode}).",
            metadata={
                "exit_code": result.returncode,
                "scan_output": result.stdout[-4000:],
                "scan_errors": result.stderr[-4000:],
            },
        )
    context.log.info("Soda checks passed successfully on Bronze tables.")
        
    return Output(
        value=result.returncode,
        metadata={
            "exit_code": result.returncode,
            "scan_output": result.stdout[:1000]
        }
    )

class QualityGateTranslator(DagsterDbtTranslator):
    def get_asset_spec(self, manifest, unique_id, project):
        spec = super().get_asset_spec(manifest, unique_id, project)
        if unique_id.startswith("model."):
            return spec.merge_attributes(deps=[soda_validate_bronze.key])
        return spec


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=QualityGateTranslator())
def ecom_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Executes dbt transformations (Staging -> Marts) and runs dbt tests."""
    yield from dbt.cli(["build", "--profiles-dir", dbt_project_dir], context=context).stream()


@asset(deps=[ecom_dbt_assets], compute_kind="soda")
def soda_validate_gold(context: AssetExecutionContext):
    """Runs Soda Core checks on the gold facts table."""
    soda_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "soda"))
    config_path = os.path.join(soda_dir, "configuration.yml")
    checks_path = os.path.join(soda_dir, "checks_gold.yml")
    
    venv_bin_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".venv", "Scripts"))
    soda_exe = os.path.join(venv_bin_dir, "soda.exe")
    cmd = [soda_exe if os.path.exists(soda_exe) else "soda", "scan", "-d", "dev", "-c", config_path, checks_path]

    context.log.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    context.log.info(result.stdout)
    
    if result.returncode == 0:
        context.log.info("Soda checks passed successfully on Gold tables.")
    elif result.returncode == 1:
        context.log.warning("Soda checks completed with warnings/failures on Gold tables.")
    else:
        context.log.error(result.stderr)
        raise Exception(f"Soda scan failed to execute: {result.stderr}")
        
    return Output(
        value=result.returncode,
        metadata={
            "exit_code": result.returncode,
            "scan_output": result.stdout[:1000]
        }
    )
