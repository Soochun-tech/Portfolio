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
