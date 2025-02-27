import contextlib

from dagster import ConfigurableResource
from sodapy import Socrata


class SocrataResource(ConfigurableResource):
    """Dagster resource for interacting with Socrata API"""
    domain: str
    app_token: str
    timeout: int = 60

    @contextlib.contextmanager
    def get_client(self) -> Socrata:
        client = Socrata(self.domain, self.app_token, timeout=self.timeout)
        try:
            yield SocrataClientWrapper(client)
        finally:
            client.close()


class SocrataClientWrapper:
    """Wrapper class for Socrata client with enhanced error handling"""

    def __init__(self, client: Socrata):
        self.client = client

    def get_dataset(
            self,
            dataset_id: str,
            limit: int = 1000,
            content_type: str = "json"
    ) -> list:
        more_data = True
        offset = 0
        while more_data:
            data = self.client.get(dataset_id,
                                   limit=limit, offset=offset,
                                   content_type=content_type)
            if not data:
                more_data = False
            else:
                yield data
                offset += limit

    def get_metadata(self, dataset_id: str) -> dict:
        return self.client.get_metadata(dataset_id)
