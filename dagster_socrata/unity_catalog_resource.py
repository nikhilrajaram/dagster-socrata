import json

import requests
from dagster import ConfigurableResource
from deltalake import DeltaTable


class UnityCatalogResource(ConfigurableResource):
    """Dagster resource for interacting Unity Catalog API"""
    endpoint_url: str
    access_token: str = ""

    @staticmethod
    def get_delta_table_columns_for_sql(delta_table: DeltaTable):
        # Get the schema information
        schema = delta_table.schema()

        def map_to_sql_type(arrow_type):
            type_str = str(arrow_type).lower()

            match type_str:
                case t if "string" in t:
                    return "STRING"
                case t if "int8" in t or "int16" in t:
                    return "SMALLINT"
                case t if "int32" in t:
                    return "INT"
                case t if "int64" in t or "int" in t:
                    return "BIGINT"
                case t if "float" in t or "double" in t:
                    return "DOUBLE"
                case t if "decimal" in t:
                    # Extract precision and scale if available
                    try:
                        precision_scale = type_str.split("(")[1].split(")")[0]
                        return f"DECIMAL({precision_scale})"
                    except IndexError:
                        return "DECIMAL(18,2)"  # Default
                case t if "date" in t:
                    return "DATE"
                case t if "timestamp" in t:
                    return "TIMESTAMP"
                case t if "boolean" in t:
                    return "BOOLEAN"
                case t if "binary" in t:
                    return "BLOB"
                case _:
                    return "STRING"  # Default fallback

        # Generate column definitions for SQL
        column_defs = []
        for field in schema.fields:
            type_str = map_to_sql_type(field.type)
            column_defs.append(
                {
                    "name": field.name,
                    "type_text": type_str.lower(),
                    "type_name": type_str,
                    "type_json": type_str,
                    "type_precision": 0,
                    "type_scale": 0,
                    "nullable": True,
                    "position": len(column_defs)
                }
            )

        return column_defs

    def delete_table_if_exists(self, catalog_name, schema_name, table_name):
        """
        Delete a table in Databricks Unity Catalog if it exists.

        Args:
            catalog_name (str): Name of parent catalog
            schema_name (str): Name of parent schema relative to its parent catalog
            table_name (str): Name of table, relative to parent schema

        Returns:
            dict: Response from the API call
        """
        endpoint = f"{self.endpoint_url}/api/2.1/unity-catalog/tables/{catalog_name}.{schema_name}.{table_name}"  # NOQA E501

        headers = {
            "Content-Type": "application/json"
        }

        if self.access_token:
            headers.update({
                "Authorization": f"Bearer {self.access_token}",
            })

        # Make the DELETE request
        response = requests.delete(
            endpoint,
            headers=headers
        )

        if response.status_code == 200:
            return True

        # Return None if the table does not exist
        if response.status_code == 404:
            return None

        # Raise an exception if the request was unsuccessful
        response.raise_for_status()

        return True

    def create_unity_catalog_table(
            self,
            dt: DeltaTable,
            name,
            catalog_name,
            schema_name,
            table_type,
            data_source_format,
            storage_location,
            comment=None,
            properties=None
    ):
        """
        Create a table in Databricks Unity Catalog.

        Args:
            dt (DeltaTable): DeltaTable object representing the table
            name (str): Name of table, relative to parent schema
            catalog_name (str): Name of parent catalog
            schema_name (str): Name of parent schema relative to its parent catalog
            table_type (str): Type of table (e.g., 'MANAGED', 'EXTERNAL')
            data_source_format (str): Format of data
                source (e.g., 'DELTA', 'PARQUET', 'CSV')
            columns (list): List of column definitions,
                each containing 'name', 'type_name', etc.
            storage_location (str): Storage root URL for external table
            comment (str, optional): User-provided free-form text description
            properties (dict, optional): A map of key-value properties
                attached to the securable

        Returns:
            dict: Response from the API call
        """
        endpoint = f"{self.endpoint_url}/api/2.1/unity-catalog/tables"

        headers = {
            "Content-Type": "application/json"
        }

        if self.access_token:
            headers.update({
                "Authorization": f"Bearer {self.access_token}",
            })

        cols = self.get_delta_table_columns_for_sql(dt)

        # Prepare the request payload according to the schema
        payload = {
            "name": name,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "table_type": table_type,
            "data_source_format": data_source_format,
            "columns": cols,
            "storage_location": storage_location
        }

        # Add optional parameters if provided
        if comment:
            payload["comment"] = comment

        if properties:
            payload["properties"] = properties

        print(json.dumps(payload, indent=2))
        # Make the POST request
        response = requests.post(
            endpoint,
            headers=headers,
            data=json.dumps(payload)
        )

        # Raise an exception if the request was unsuccessful
        response.raise_for_status()

        # Return the response as a dictionary
        return response.json()
