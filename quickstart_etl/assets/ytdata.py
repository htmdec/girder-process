import base64
from io import BytesIO
import os
from typing import List
import pandas as pd

import requests
import girder_client

from dagster import AssetExecutionContext, MetadataValue, asset


@asset(name="gc", group_name="yt_sample_data", compute_kind="yt data collection")
def auth_girder_client(context: AssetExecutionContext) -> girder_client.GirderClient:
    gc = girder_client.GirderClient(apiUrl="https://girder.hub.yt/api/v1")
    rv = gc.authenticate(apiKey=os.getenv("GIRDER_API_KEY"))
    context.add_output_metadata({"user_id": rv["_id"]})
    return gc


@asset(group_name="yt_sample_data", compute_kind="yt data collection")
def current_yt_sample_data(gc: girder_client.GirderClient) -> List[str]:
    """Get the current IDs of items in the yt_sample_data collection on hub.yt."""
    results = []
    for folder in gc.listFolder("577bf2ba0d7c6b0001ad4a4b"):
        for item in gc.listItem(folder["_id"]):
            results.append(item["_id"])
    return results


@asset(group_name="yt_sample_data", compute_kind="yt data collection")
def item_information(
    context: AssetExecutionContext,
    current_yt_sample_data: List[str],
    gc: girder_client.GirderClient,
) -> pd.DataFrame:
    """Get the information about each item and its contents."""
    results = []
    for item_id in current_yt_sample_data:
        results.append(gc.getItem(item_id))
    df = pd.DataFrame(results)
    context.add_output_metadata(
        {
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )
    return df
