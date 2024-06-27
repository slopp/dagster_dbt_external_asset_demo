import json
from pathlib import Path

from dagster import Definitions, external_assets_from_specs
from dagster_dbt import build_dbt_asset_specs

project_dir = Path(__file__).joinpath("..","jaffle_shop_duckdb").resolve()
manifest = json.loads(project_dir.joinpath("target", "manifest.json").read_bytes())

jaffle_shop_dbt_assets = external_assets_from_specs(
    build_dbt_asset_specs(manifest=manifest)
)


defs = Definitions(assets=[*jaffle_shop_dbt_assets])