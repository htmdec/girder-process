from contextlib import contextmanager
from dagster import ConfigurableResource
import girder_client
from pydantic import PrivateAttr


class GirderCredentials(ConfigurableResource):
    api_key: str


class GirderConnection(ConfigurableResource):
    credentials: GirderCredentials
    url: str
    _client: girder_client.GirderClient = PrivateAttr()

    @contextmanager
    def yield_for_execution(self, context):
        self._client = girder_client.GirderClient(apiUrl="https://girder.hub.yt/api/v1")
        self._client.authenticate(apiKey=self.credentials.api_key)
        yield self

    def list_folder(self, folder_id):
        return list(self._client.listFolder(folder_id))

    def list_item(self, folder_id):
        return list(self._client.listItem(folder_id))

    def get_item(self, item_id):
        return self._client.getItem(item_id)
