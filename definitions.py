import json
from pathlib import Path

from dagster import Definitions, external_assets_from_specs, AssetSpec, AssetKey
from dagster_dbt import build_dbt_asset_specs

# create an external asset for one of the dbt sources
# see https://docs.dagster.io/concepts/assets/external-assets

source_assets = external_assets_from_specs(
    [
        AssetSpec(
            key=AssetKey(["main", "raw_locations"]),
            description="locations table created by some external job that loads raw locations data",
            tags={"dagster/storage_kind": "duckdb"},
        )
    ]
)

# load external assets from dbt manifest
project_dir = Path(__file__).joinpath("..", "jaffle_shop_duckdb").resolve()
manifest = json.loads(project_dir.joinpath("target", "manifest.json").read_bytes())

jaffle_shop_dbt_assets = external_assets_from_specs(
    build_dbt_asset_specs(manifest=manifest)
)


defs = Definitions(assets=[*source_assets, *jaffle_shop_dbt_assets])
