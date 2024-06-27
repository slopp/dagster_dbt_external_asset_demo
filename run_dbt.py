import os
from pathlib import Path

import httpx
from dbt.cli.main import dbtRunner, dbtRunnerResult

REPORT_ASSET_MATERIALIZATION_URL = "http://localhost:3000/report_asset_materialization/"

def report_dbt_metadata_to_dagster(dbt_model_asset_key, metadata):
    httpx.post(
                url=REPORT_ASSET_MATERIALIZATION_URL,
                json={
                    "asset_key": dbt_model_asset_key,
                    "metadata": metadata
                }
            )
    return 


def run_dbt_and_report_metadata():

    project_dir = Path(__file__).joinpath("..", "jaffle_shop_duckdb").resolve()
    

    # run dbt however you want, here based on https://docs.getdbt.com/reference/programmatic-invocations
    dbt = dbtRunner()
    cli_args = ["build", "--project-dir", project_dir, "--profiles-dir", project_dir]
    res: dbtRunnerResult = dbt.invoke(cli_args)

    if res.exception is None:
        for result in res.result.results:
            if result.node.resource_type in ['model', 'seed']:
                name = result.node.identifier
                execution_time = result.execution_time
                column_names = list(result.node.columns.keys())
                metadata = {
                    "execution_time": execution_time,
                    "column_names": column_names
                }

                report_dbt_metadata_to_dagster(name, metadata)

if __name__ == "__main__":
    run_dbt_and_report_metadata()