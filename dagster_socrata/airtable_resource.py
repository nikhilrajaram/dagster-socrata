from datetime import datetime

from dagster import ConfigurableResource
from pyairtable import Api


class AirTableResource(ConfigurableResource):
    """Dagster resource for interacting Unity Catalog API"""

    api_key: str = "XXXX"
    base_id: str = ""
    table_id: str = ""

    def get_schema(self):
        """Get all tables from Airtable"""
        api = Api(self.api_key)
        table = api.table(self.base_id, self.table_id)
        return table.schema()

    def create_table_record(
        self,
        catalog: str,
        schema: str,
        table: str,
        name: str,
        deltalake_path: str,
        description: str,
        license: str,
        pub_date: datetime,
    ):
        """Create a record in the table"""
        api = Api(self.api_key)
        table_ref = api.table(self.base_id, self.table_id)
        table_ref.create(
            {
                "Catalog": catalog,
                "Schema": schema,
                "TableName": table,
                "Name": name,
                "Description": description,
                "DeltaTablePath": deltalake_path,
                "License": license,
                "PublicationDate": pub_date.strftime("%Y-%m-%d"),
            }
        )
