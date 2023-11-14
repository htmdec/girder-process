from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
    EnvVar,
)

from . import assets
from .resources.girder import GirderConnection, GirderCredentials

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
)

defs = Definitions(
    assets=load_assets_from_package_module(assets),
    schedules=[daily_refresh_schedule],
    resources={
        "girder_connection": GirderConnection(
            credentials=GirderCredentials(api_key=EnvVar("GIRDER_API_KEY")),
            url=EnvVar("GIRDER_URL"),
        )
    },
)
