from ..resources.girder import GirderConnection
from dagster import sensor, DynamicPartitionsDefinition, RunRequest, SensorResult


def make_collection_contents_job(
    job, folder_id, partitions_def: DynamicPartitionsDefinition
):
    @sensor(name=f"{job.name}_on_collection_contents", job=job)
    def collection_contents(context, girder_connection: GirderConnection):
        new_items = []
        for folder in girder_connection.list_folder(folder_id):
            for item in girder_connection.list_item(folder["_id"]):
                if not context.instance.has_dynamic_partition(
                    partitions_def.name, item["name"]
                ):
                    new_items.append(item)
        run_requests = []
        for item in new_items:
            tags = partitions_def.get_tags_for_partition_key(item["name"])
            tags.update(item)
            run_requests.append(RunRequest(partition_key=item["name"], tags=tags))
        return SensorResult(
            run_requests=run_requests,
            dynamic_partitions_requests=[
                partitions_def.build_add_request([_["name"] for _ in new_items])
            ],
        )

    return collection_contents
