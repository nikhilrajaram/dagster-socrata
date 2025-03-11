import contextlib
import json
from typing import Any
import pyarrow as pa
from dagster import ConfigurableResource
from sodapy import Socrata
from dagster_ncsa import S3ResourceNCSA


class SocrataMetadata:
    def __init__(self, metadata: dict[str, Any]):
        self.metadata = metadata

    def __getitem__(self, key):
        # Allow accessing dictionary items using obj['key']
        return self.metadata[key]

    def __str__(self):
        return json.dumps(self.metadata, indent=2)

    def save(self, bucket_name, s3_path, s3: S3ResourceNCSA) -> str:
        metadata_path = f"{s3_path.rstrip('/')}/metadata.json"
        s3.save_json_object(bucket_name, metadata_path, self.metadata)
        return metadata_path

    @classmethod
    def read(cls, s3: S3ResourceNCSA, bucket_name, metadata_path):
        return s3.read_json_object(bucket_name, metadata_path)

    @staticmethod
    def infer_type(column: dict[str, Any], schema_or_sql="sql"):
        int_type = "INTEGER" if schema_or_sql == "sql" else pa.int64()
        float_type = "FLOAT" if schema_or_sql == "sql" else pa.float64()
        text_type = "VARCHAR" if schema_or_sql == "sql" else pa.string()
        date_type = "TIMESTAMP" if schema_or_sql == "sql" else pa.timestamp("s")
        bool_type = "BOOLEAN" if schema_or_sql == "sql" else pa.bool_()

        def all_items_are_integers(data):
            if "top" not in data:
                return False
            try:
                return all(int(entry["item"]) or True for entry in data["top"])
            except (ValueError, KeyError):
                return False

        if column["dataTypeName"] == "number":
            if "cachedContents" in column:
                return (
                    int_type
                    if all_items_are_integers(column["cachedContents"])
                    else float_type
                )
            else:
                return float_type
        elif column["dataTypeName"] == "text":
            return text_type
        elif column["dataTypeName"] == "date":
            return date_type
        elif column["dataTypeName"] == "boolean":
            return bool_type
        else:
            return text_type

    @property
    def schema(self):
        return pa.schema(
            [
                (col["fieldName"], self.infer_type(col, "pa"))
                for col in self.metadata["columns"]
            ]
        )

    @property
    def casts(self):
        casts = [
            f"CAST({col['fieldName']} AS {self.infer_type(col, 'sql')}) AS {col['fieldName']}"  # NOQA E501
            for col in self.metadata["columns"]
        ]
        return ", ".join(casts)

    @property
    def table_name(self):
        return self.metadata["id"].replace("-", "_")

    @property
    def license(self):
        return self.metadata.get("license", {}).get("name", "Unknown")


class SocrataClientWrapper:
    """Wrapper class for Socrata client with enhanced error handling"""

    def __init__(self, client: Socrata):
        self.client = client

    def get_dataset(
        self, dataset_id: str, limit: int = 1000, content_type: str = "json"
    ) -> list:
        more_data = True
        offset = 0
        while more_data:
            data = self.client.get(
                dataset_id, limit=limit, offset=offset, content_type=content_type
            )
            if not data:
                more_data = False
            else:
                yield data
                offset += limit

    def get_metadata(self, dataset_id: str) -> SocrataMetadata:
        return SocrataMetadata(self.client.get_metadata(dataset_id))


class SocrataResource(ConfigurableResource):
    """Dagster resource for interacting with Socrata API"""

    domain: str
    app_token: str
    timeout: int = 60

    @contextlib.contextmanager
    def get_client(self) -> SocrataClientWrapper:
        client = Socrata(self.domain, self.app_token, timeout=self.timeout)
        try:
            yield SocrataClientWrapper(client)
        finally:
            client.close()
