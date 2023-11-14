from typing import List
import pandas as pd

from dagster import AssetExecutionContext, MetadataValue, asset
from ..resources.girder import GirderConnection


@asset(group_name="yt_sample_data", compute_kind="yt data collection")
def current_yt_sample_data(girder_connection: GirderConnection) -> List[str]:
    """Get the current IDs of items in the yt_sample_data collection on hub.yt."""
    results = []
    for folder in girder_connection.list_folder("577bf2ba0d7c6b0001ad4a4b"):
        for item in girder_connection.list_item(folder["_id"]):
            results.append(item["_id"])
    return results


@asset(group_name="yt_sample_data", compute_kind="yt data collection")
def item_information(
    context: AssetExecutionContext,
    current_yt_sample_data: List[str],
    girder_connection: GirderConnection,
) -> pd.DataFrame:
    """Get the information about each item and its contents."""
    results = []
    for item_id in current_yt_sample_data:
        results.append(girder_connection.get_item(item_id))
    df = pd.DataFrame(results)
    context.add_output_metadata(
        {
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )
    return df
