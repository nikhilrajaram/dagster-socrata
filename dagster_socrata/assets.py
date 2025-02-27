import json
from io import BytesIO
from logging import Logger
from typing import Any, Dict, List

import pyarrow
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from dagster import AssetExecutionContext, Config, EnvVar, asset
from dagster_aws.s3 import S3Resource

from dagster_socrata.resource import SocrataResource


class SocrataDatasetConfig(Config):
    dataset_id: str = "vdgb-f9s3"
    socrata_batch_size: int = 10000
    parquet_chunk_size: int = int(1e6)


@asset(
    compute_kind="socrata",
    group_name="data_ingestion"
)
def socrata_dataset(context: AssetExecutionContext,
                    config: SocrataDatasetConfig,
                    socrata: SocrataResource,
                    s3: S3Resource) -> None:
    """Asset that streams data from Socrata directly to S3"""
    bucket_name = EnvVar("DEST_BUCKET").get_value()
    context.log.info(f"Writing to {bucket_name}")

    s3_key = f"parquet/{socrata.domain}/{config.dataset_id}"
    delete_directory_in_s3(bucket_name, s3_key, s3)
    s3_client = s3.get_client()

    with socrata.get_client() as client:
        metadata = client.get_metadata(config.dataset_id)
        s3_client.put_object(Bucket=bucket_name,
                             Key=f"{s3_key}/metadata.json",
                             Body=json.dumps(metadata))

        with JSONBatchS3Writer(bucket_name=bucket_name,
                               prefix=s3_key,
                               batch_size=config.parquet_chunk_size,
                               log=context.log,
                               s3=s3) as writer:
            for json_records in client.get_dataset(
                    config.dataset_id, limit=config.socrata_batch_size):
                writer.add(json_records)


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


class JSONBatchS3Writer:
    """
    A context manager that batches JSON documents and writes them to S3.
    It collects documents until the batch size is reached, then writes to S3.
    Any remaining documents are written when the context is exited.
    """

    def __init__(self,
                 bucket_name: str,
                 prefix: str,
                 batch_size: int = 5,
                 log: Logger = None,
                 s3: S3Resource = None):
        """
        Initialize the batch writer.

        Args:
            bucket_name: The S3 bucket name
            prefix: The S3 key prefix to use for the files
            batch_size: Number of documents to batch before writing (default: 5)
            s3: Dagster S3 Resource
        """
        self.bucket = bucket_name
        self.prefix = prefix
        self.batch_size = batch_size
        self.batch: List[Dict[str, Any]] = []
        self.batch_count = 0
        self.s3 = s3.get_client()
        self.log = log

    def __enter__(self):
        """Enter the context manager and return self."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context manager, ensuring any remaining
        documents in the batch are written to S3.
        """
        if self.batch:
            self._write_batch()

    def add(self, doc: List[Dict[str, Any]]) -> None:
        """
        Add a document to the batch. If the batch size is reached,
        write the batch to S3.

        Args:
            doc: The JSON-serializable document to add
        """
        self.batch.extend(doc)

        if len(self.batch) >= self.batch_size:
            self._write_batch()

    def _write_batch(self) -> None:
        """Write the current batch of documents to S3."""
        if not self.batch:
            return

        self.log.info(f"Writing batch {self.batch_count} of size {len(self.batch)} to S3")
        table = pyarrow.Table.from_pylist(self.batch)
        s3_key = f"{self.prefix}/part-{self.batch_count:03d}.parquet"
        buffer = BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)

        self.s3.upload_fileobj(
            buffer,
            self.bucket,
            s3_key)

        # Reset batch and increment batch counter
        self.batch = []
        self.batch_count += 1
