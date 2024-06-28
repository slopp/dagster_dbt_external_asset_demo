This is a slightly modified version of the dbt jaffle_shop_duckdb example.

The modifications are:
-  the addition of the file `models/locations.sql`
-  the addition of the file `models/sources.yml` 

These two files have been added to allow the Dagster project to represent the lineage between an external source asset and a downstream dbt model using `source("table")`.