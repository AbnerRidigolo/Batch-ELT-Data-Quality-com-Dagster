import duckdb
from dagster import AssetKey, asset, materialize
from dagster_dbt import DbtCliResource

from dagster_project.assets import dbt_project, ecom_dbt_assets, soda_validate_bronze
from dagster_project.definitions import defs


def test_every_dbt_model_waits_for_bronze_approval():
    gate = AssetKey("soda_validate_bronze")
    assert all(gate in deps for deps in ecom_dbt_assets.asset_deps.values())


def test_invalid_bronze_blocks_dbt(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    data = tmp_path / "data"
    data.mkdir()
    with duckdb.connect(str(data / "analytics.duckdb")) as connection:
        connection.execute("create table bronze_customers as select 1 as id")
        connection.execute(
            "create table bronze_sales as select 2 as sale_id, null::decimal(12, 2) as amount"
        )

    @asset(key_prefix="raw")
    def bronze_customers():
        pass

    @asset(key_prefix="raw")
    def bronze_sales():
        pass

    result = materialize(
        [bronze_customers, bronze_sales, soda_validate_bronze, ecom_dbt_assets],
        resources={"dbt": DbtCliResource(project_dir=dbt_project)},
        raise_on_error=False,
    )
    assert not result.success
    (failure,) = result.get_step_failure_events()
    assert failure.step_key == "soda_validate_bronze"
    metadata = failure.step_failure_data.user_failure_data.metadata
    assert metadata["exit_code"].value == 2
    assert "Amount should not be null" in metadata["scan_output"].value
    assert not any(
        event.is_step_start and event.step_key == ecom_dbt_assets.node_def.name
        for event in result.all_events
    )


def test_valid_sample_reaches_gold():
    result = defs.resolve_job_def("elt_pipeline_job").execute_in_process()
    assert result.success
    materialized = {
        event.asset_key
        for event in result.get_asset_materialization_events()
    }
    assert AssetKey("soda_validate_gold") in materialized
    assert ecom_dbt_assets.keys <= materialized
