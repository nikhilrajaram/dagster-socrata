# Mock data for testing
import json
import os
from contextlib import contextmanager
from unittest.mock import MagicMock

from dagster_socrata.socrata_resource import SocrataMetadata

MOCK_DATASET_ID = "rkpp-igza"


class MockSocrataResource:
    def __init__(self, csv_data: list[list[list[str]]] = None):
        self.csv_data = csv_data
        # Get the directory where the script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # Create the full path to the JSON file
        json_path = os.path.join(script_dir, 'metadata.json')

        with open(json_path, 'r') as file:
            self.metadata = SocrataMetadata(json.load(file))

        self.client = MagicMock()

    @contextmanager
    def get_client(self):
        self.client.get_metadata.return_value = self.metadata
        self.client.get_dataset.side_effect = [self.csv_data]
        yield self.client
