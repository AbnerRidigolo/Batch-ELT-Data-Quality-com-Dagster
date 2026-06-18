import os
from dagster import Definitions, define_asset_job
from dagster_dbt import DbtCliResource
from .assets import (
    bronze_raw_data,
    bronze_customers,
    bronze_sales,
    soda_validate_bronze,
    ecom_dbt_assets,
    soda_validate_gold,
    dbt_project
)

# Setup dbt CLI resource for Dagster
dbt_resource = DbtCliResource(project_dir=dbt_project)

# Define a job that materializes all software-defined assets in the pipeline selection
elt_pipeline_job = define_asset_job(
    name="elt_pipeline_job",
    selection="*"
)

# Bundle everything into the Definitions object
defs = Definitions(
    assets=[
        bronze_raw_data,
        bronze_customers,
        bronze_sales,
        soda_validate_bronze,
        ecom_dbt_assets,
        soda_validate_gold
    ],
    resources={
        "dbt": dbt_resource,
    },
    jobs=[elt_pipeline_job]
)
