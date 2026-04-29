"""
Dagster Definitions — entry point that registers all assets, asset checks,
and resources for this code location.

Run the local dev UI from the project root:
    dagster dev -m dagster_project.definitions

Then open http://localhost:3000.
"""
from __future__ import annotations

from dagster import Definitions, load_assets_from_modules

from .assets import taxi as taxi_assets
from .assets import olist as olist_assets
from .resources import make_mysql_resource

ASSET_MODULES = [taxi_assets, olist_assets]

defs = Definitions(
    assets=load_assets_from_modules(ASSET_MODULES),
    resources={
        "mysql": make_mysql_resource(),
    },
)
