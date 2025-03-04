import io
import json
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import s3fs
from botocore.exceptions import ClientError
from dagster import AssetExecutionContext, Config, EnvVar
from dagster import AssetIn, Output, asset
from dagster_aws.s3 import S3Resource
from deltalake import DeltaTable
from deltalake import write_deltalake

from dagster_socrata.airtable_resource import AirTableResource
from dagster_socrata.resource import SocrataResource


class SocrataMetadata:
    def __init__(self, metadata: Dict[str, Any]):
        self.metadata = metadata

    def __getitem__(self, key):
        # Allow accessing dictionary items using obj['key']
        return self.metadata[key]

    def save(self, s3_client, bucket_name, s3_path) -> str:
        metadata_path = f"{s3_path.rstrip('/')}/metadata.json"
        s3_client.put_object(Bucket=bucket_name,
                             Key=metadata_path,
                             Body=json.dumps(self.metadata))
        return metadata_path

    @classmethod
    def read(cls, s3_client, bucket_name, metadata_path):
        response = s3_client.get_object(Bucket=bucket_name, Key=metadata_path)
        return cls(json.loads(response['Body'].read()))

    @staticmethod
    def infer_type(column: Dict[str, Any], schema_or_sql='sql'):
        int_type = "INTEGER" if schema_or_sql == 'sql' else pa.int64()
        float_type = "FLOAT" if schema_or_sql == 'sql' else pa.float64()
        text_type = "VARCHAR" if schema_or_sql == 'sql' else pa.string()
        date_type = "TIMESTAMP" if schema_or_sql == 'sql' else pa.timestamp('s')
        bool_type = "BOOLEAN" if schema_or_sql == 'sql' else pa.bool_()

        def all_items_are_integers(data):
            if "top" not in data:
                return False
            try:
                return all(int(entry["item"]) or True for entry in data["top"])
            except (ValueError, KeyError):
                return False

        if column['dataTypeName'] == 'number':
            if "cachedContents" in column:
                return int_type\
                    if all_items_are_integers(column["cachedContents"]) \
                    else float_type
            else:
                return float_type
        elif column['dataTypeName'] == 'text':
            return text_type
        elif column['dataTypeName'] == 'date':
            return date_type
        elif column['dataTypeName'] == 'boolean':
            return bool_type
        else:
            return text_type

    @property
    def schema(self):
        return pa.schema(
            [(col['fieldName'], self.infer_type(col, "pa"))
             for col in self.metadata['columns']])

    @property
    def casts(self):
        casts = [f"CAST({col['fieldName']} AS {self.infer_type(col, 'sql')}) AS {col['fieldName']}"  # NOQA E501
                 for col in self.metadata['columns']]
        return ", ".join(casts)

    @property
    def table_name(self):
        return self.metadata['id'].replace("-", "_")

    @property
    def license(self):
        return self.metadata.get('license', {}).get('name', 'Unknown')


class SocrataDatasetConfig(Config):
    dataset_id: str = "vdgb-f9s3"


@asset(
    group_name="CDC"
)
def socrata_metadata(context: AssetExecutionContext,
                     config: SocrataDatasetConfig,
                     socrata: SocrataResource) -> SocrataMetadata:
    """Asset that retrieves metadata from Socrata"""
    with socrata.get_client() as client:
        metadata_json = client.get_metadata(config.dataset_id)
        context.log.info(f"Retrieved metadata for {config.dataset_id}")
        return SocrataMetadata(metadata_json)


class SocrataAPIConfig(Config):
    socrata_batch_size: int = 2000


@asset(
    ins={"socrata_metadata": AssetIn()},
    group_name="CDC",
    description="Downloads Socrata dataset and writes it to an object store as CSV",
)
def socrata_to_object_store(context,
                            socrata_metadata,
                            config: SocrataAPIConfig,
                            socrata: SocrataResource,
                            s3: S3Resource) -> Output:
    """
    Asset that downloads a Socrata dataset based on metadata and writes it to an object
    store as CSV.

    Args:
        context: The Dagster execution context
        socrata_metadata: Metadata from the socrata_metadata asset

    Returns:
        Information about the dataset stored in the object store
    """
    # Extract dataset information from metadata
    dataset_id = socrata_metadata["id"]
    stage_path = f"stage/{dataset_id}/"

    # Log the operation
    context.log.info(f"Processing Socrata dataset: {dataset_id}")
    bucket_name = EnvVar("DEST_BUCKET").get_value()
    s3_client = s3.get_client()
    delete_directory_in_s3(bucket_name, stage_path, s3)

    context.log.info("Saving metadata to " + stage_path)
    socrata_metadata.save(s3_client, bucket_name, stage_path)

    # Access the SocrataResource and get the dataset
    with socrata.get_client() as client:
        part = 0
        for data in client.get_dataset(dataset_id, limit=config.socrata_batch_size):
            # Extract the headers (first row)
            headers = data[0]

            # Extract the data (remaining rows)
            rows = data[1:]
            df = pd.DataFrame(rows, columns=headers)
            # Convert DataFrame to CSV
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            s3_key = f"stage/{dataset_id}/PART-{part:03d}.csv"

            # Write the CSV to the object store
            s3_client.put_object(Bucket=EnvVar("DEST_BUCKET").get_value(),
                                 Key=s3_key,
                                 Body=csv_data)

            part += 1

    # Return information about the stored dataset
    return Output(
        value={
            "csv_path": stage_path,
            "socrata_domain": socrata.domain,
        },
        metadata={
            "dataset_id": dataset_id,
            "license": socrata_metadata.license,
        }
    )


class SocrataToDeltaLakeConfig(Config):
    min_row_group_size: int = 0       # Sets min rows per row group
    max_row_group_size: int = 100000  # Sets max rows per row group


@asset(
    ins={"socrata_metadata": AssetIn(),
         "socrata_to_object_store": AssetIn()},
    group_name="CDC",
    description="Converts Socrata dataset to Delta Lake"
)
def socrata_to_deltalake(context: AssetExecutionContext,
                         config: SocrataToDeltaLakeConfig,
                         socrata_to_object_store: Dict[str, Any],
                         socrata_metadata: SocrataMetadata,
                         s3: S3Resource) -> Output:

    bucket = EnvVar("DEST_BUCKET").get_value()
    delta_table_path = f"s3://{bucket}/delta/{socrata_to_object_store['socrata_domain']}/{socrata_metadata['id']}"  # NOQA E501

    storage_options = {"AWS_ACCESS_KEY_ID": s3.aws_access_key_id,
                       "AWS_SECRET_ACCESS_KEY": s3.aws_secret_access_key,
                       "AWS_ENDPOINT_URL": s3.endpoint_url}

    fs = s3fs.S3FileSystem(key=s3.aws_access_key_id,
                           secret=s3.aws_secret_access_key,
                           endpoint_url=s3.endpoint_url)

    # Create a list of s3fs paths to the CSV files downloaded
    # from socrata
    csv_files = [f"/{bucket}/{file}" for file in
                 list_csv_files(bucket,
                                socrata_to_object_store['csv_path'],
                                s3)]

    # Create a pyarrow dataset referencing all of our csv files
    dataset = ds.dataset(csv_files, format="csv",
                         filesystem=fs, exclude_invalid_files=True,
                         schema=socrata_metadata.schema)

    write_deltalake(delta_table_path, dataset,
                    name=socrata_metadata.table_name,
                    description=socrata_metadata['name'],
                    min_rows_per_group=config.min_row_group_size,
                    max_rows_per_group=config.max_row_group_size,
                    mode="overwrite",
                    storage_options=storage_options,
                    schema=socrata_metadata.schema)

    delta_table = DeltaTable(delta_table_path, storage_options=storage_options)

    # Optimize the table - this should create a checkpoint file
    delta_table.create_checkpoint()
    print(delta_table.file_uris())

    return Output(
        value={
            "delta_table_path": delta_table_path,
            "table_name": socrata_metadata.table_name
        },
        metadata={
            "table_name": socrata_metadata.table_name,
            "table_description": socrata_metadata['name'],
            "delta_table_path": delta_table_path
        }
    )


class CatalogConfig(Config):
    catalog: str = "PublicHealth"
    schema: str = "sdoh"


@asset(
    group_name="CDC",
    description="Create Entry in Data Catalog",
    ins={"socrata_metadata": AssetIn(),
         "socrata_to_deltalake": AssetIn()},
)
def create_entry_in_data_catalog(context: AssetExecutionContext,
                                 config: CatalogConfig,
                                 airtable: AirTableResource,
                                 socrata_metadata, socrata_to_deltalake):
    """
    Create an entry in the data catalog for the dataset.
    :param context:
    :param config:
    :param socrata_metadata:
    :param socrata_to_deltalake:
    :return:
    """
    airtable.create_table_record(
        catalog=config.catalog,
        schema=config.schema,
        table=socrata_metadata.table_name,
        name=socrata_metadata['name'],
        description=socrata_metadata['description'],
        deltalake_path=socrata_to_deltalake['delta_table_path'],
        license=socrata_metadata.license,
        pub_date=datetime.fromtimestamp(socrata_metadata['publicationDate'])
    )


def delete_directory_in_s3(bucket_name, directory_path, s3: S3Resource):
    """
    Delete all files in a directory in an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        directory_path (str): The directory path in the bucket (without leading slash).
                              Make sure it ends with a '/' if it's a directory.
        s3 (S3Resource): Dagster S3 Resource

    Returns:
        dict: Information about the operation result.
    """
    # Ensure directory path ends with a slash
    if not directory_path.endswith('/'):
        directory_path += '/'

    # Initialize S3 client
    s3_client = s3.get_client()

    try:
        # List all objects in the directory
        paginator = s3_client.get_paginator('list_objects_v2')
        objects_to_delete = []

        # Use pagination to handle large directories
        for page in paginator.paginate(Bucket=bucket_name, Prefix=directory_path):
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects_to_delete.append({'Key': obj['Key']})

        # If no objects found
        if not objects_to_delete:
            return {
                'success': True,
                'message': f"No objects found in {directory_path}",
                'deleted_count': 0
            }

        # Delete the objects
        response = s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': objects_to_delete,
                'Quiet': False
            }
        )

        return {
            'success': True,
            'message': f"Successfully deleted {len(objects_to_delete)} objects from {directory_path}",  # NOQA E501
            'deleted_count': len(objects_to_delete),
            'deleted_objects': [obj['Key'] for obj in objects_to_delete],
            'response': response
        }

    except ClientError as e:
        return {
            'success': False,
            'message': f"Error: {str(e)}",
            'error': e
        }


def list_csv_files(bucket_name, directory_path: str, s3: S3Resource) -> List[str]:
    if not directory_path.endswith('/'):
        directory_path += '/'

    # Initialize S3 client
    s3_client = s3.get_client()

    try:
        # List all objects in the directory
        paginator = s3_client.get_paginator('list_objects_v2')
        objects = []

        # Use pagination to handle large directories
        for page in paginator.paginate(Bucket=bucket_name, Prefix=directory_path):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.csv'):
                        objects.append(obj['Key'])
        return objects

    except ClientError as e:
        raise e
