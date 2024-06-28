import os
from pathlib import Path
import duckdb 
import httpx
from dbt.cli.main import dbtRunner, dbtRunnerResult

# see https://docs.dagster.io/apidocs/external-assets-rest
REPORT_ASSET_MATERIALIZATION_URL = "http://localhost:3000/report_asset_materialization/"

def report_dbt_metadata_to_dagster(dbt_model_asset_key, metadata):
    """ report metadata to dagster about externally managed assets """
    httpx.post(
                url=REPORT_ASSET_MATERIALIZATION_URL,
                json={
                    "asset_key": dbt_model_asset_key,
                    "metadata": metadata
                }
            )
    return 


def create_duckdb_locations_table():
    """ Create a table that will be used as a dbt source """
    con = duckdb.connect(database="./jaffle_shop_duckdb/jaffle_shop.duckdb", read_only=False)
    con.execute(
        """ 
            CREATE TABLE locations AS 
                SELECT * FROM read_csv("./jaffle_shope_duckdb/seeds/raw_customeres.csv")
        
        """
    )

    report_dbt_metadata_to_dagster("locations", {})

def run_dbt_and_report_metadata():
    """ Run dbt project and report the results to dagster """

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
    create_duckdb_locations_table()
    run_dbt_and_report_metadata()