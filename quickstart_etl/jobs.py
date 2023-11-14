from dagster import AssetSelection, define_asset_job
from .partitions import items_partitions_def
from .sensors.collection_contents import make_collection_contents_job

items_assets_sensor = make_collection_contents_job(
    define_asset_job(
        "items_process",
        selection=AssetSelection.groups("yt_data_items"),
        partitions_def=items_partitions_def,
    ),
    "577bf2ba0d7c6b0001ad4a4b",
    items_partitions_def,
)
