import io
from datetime import datetime
from typing import Any, Dict

import pandas as pd
import pyarrow.dataset as ds
import s3fs
from dagster import AssetExecutionContext, Config, EnvVar
from dagster import AssetIn, Output, asset
from deltalake import DeltaTable
from deltalake import write_deltalake

from dagster_ncsa.airtable_catalog_resource import AirTableCatalogResource
from dagster_ncsa.s3_resource_ncsa import S3ResourceNCSA
from dagster_socrata.socrata_resource import SocrataMetadata, SocrataResource


class SocrataDatasetConfig(Config):
    dataset_id: str = "vdgb-f9s3"


@asset(
    group_name="Socrata",
    description="Retrieves metadata from Socrata",
)
def socrata_metadata(
    context: AssetExecutionContext,
    config: SocrataDatasetConfig,
    socrata: SocrataResource,
) -> SocrataMetadata:
    """Asset that retrieves metadata from Socrata"""
    with socrata.get_client() as client:
        metadata = client.get_metadata(config.dataset_id)
        context.log.info(f"Retrieved metadata for \"{metadata['name']}\"")
        return metadata


class SocrataAPIConfig(Config):
    socrata_batch_size: int = 2000


@asset(
    ins={"socrata_metadata": AssetIn()},
    group_name="Socrata",
    description="Downloads Socrata dataset and writes it to an object store as CSV",
)
def socrata_to_object_store(
    context,
    socrata_metadata,
    config: SocrataAPIConfig,
    socrata: SocrataResource,
    s3: S3ResourceNCSA,
) -> Output:
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
    s3.delete_directory(bucket_name, stage_path)

    context.log.info("Saving metadata to " + stage_path)
    socrata_metadata.save(bucket_name, stage_path, s3)

    s3_client = s3.get_client()
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
            s3_client.put_object(
                Bucket=EnvVar("DEST_BUCKET").get_value(), Key=s3_key, Body=csv_data
            )

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
        },
    )


class SocrataToDeltaLakeConfig(Config):
    min_row_group_size: int = 0  # Sets min rows per row group
    max_row_group_size: int = 100000  # Sets max rows per row group


@asset(
    ins={"socrata_metadata": AssetIn(), "socrata_to_object_store": AssetIn()},
    group_name="CDC",
    description="Converts Socrata dataset to Delta Lake",
)
def socrata_to_deltalake(
    context: AssetExecutionContext,
    config: SocrataToDeltaLakeConfig,
    socrata_to_object_store: Dict[str, Any],
    socrata_metadata: SocrataMetadata,
    s3: S3ResourceNCSA,
) -> Output:

    bucket = EnvVar("DEST_BUCKET").get_value()
    delta_table_path = f"s3://{bucket}/delta/{socrata_to_object_store['socrata_domain']}/{socrata_metadata['id']}"  # NOQA E501

    storage_options = {
        "AWS_ACCESS_KEY_ID": s3.aws_access_key_id,
        "AWS_SECRET_ACCESS_KEY": s3.aws_secret_access_key,
        "AWS_ENDPOINT_URL": s3.endpoint_url,
    }

    fs = s3fs.S3FileSystem(
        key=s3.aws_access_key_id,
        secret=s3.aws_secret_access_key,
        endpoint_url=s3.endpoint_url,
    )

    # Create a list of s3fs paths to the CSV files downloaded
    # from socrata
    csv_files = [
        f"/{bucket}/{file}"
        for file in s3.list_files(bucket, socrata_to_object_store["csv_path"], ".csv")
    ]

    # Create a pyarrow dataset referencing all of our csv files
    dataset = ds.dataset(
        csv_files,
        format="csv",
        filesystem=fs,
        exclude_invalid_files=True,
        schema=socrata_metadata.schema,
    )

    write_deltalake(
        delta_table_path,
        dataset,
        name=socrata_metadata.table_name,
        description=socrata_metadata["name"],
        min_rows_per_group=config.min_row_group_size,
        max_rows_per_group=config.max_row_group_size,
        mode="overwrite",
        storage_options=storage_options,
        schema=socrata_metadata.schema,
    )

    delta_table = DeltaTable(delta_table_path, storage_options=storage_options)

    # Optimize the table - this should create a checkpoint file
    delta_table.create_checkpoint()
    print(delta_table.file_uris())

    return Output(
        value={
            "delta_table_path": delta_table_path,
            "table_name": socrata_metadata.table_name,
        },
        metadata={
            "table_name": socrata_metadata.table_name,
            "table_description": socrata_metadata["name"],
            "delta_table_path": delta_table_path,
        },
    )


class CatalogConfig(Config):
    catalog: str = "PublicHealth"
    schema: str = "sdoh"


@asset(
    group_name="CDC",
    description="Create Entry in Data Catalog",
    ins={"socrata_metadata": AssetIn(), "socrata_to_deltalake": AssetIn()},
)
def create_entry_in_data_catalog(
    context: AssetExecutionContext,
    config: CatalogConfig,
    airtable: AirTableCatalogResource,
    socrata_metadata,
    socrata_to_deltalake,
):
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
        name=socrata_metadata["name"],
        description=socrata_metadata["description"],
        deltalake_path=socrata_to_deltalake["delta_table_path"],
        license_name=socrata_metadata.license,
        pub_date=datetime.fromtimestamp(socrata_metadata["publicationDate"]),
    )
