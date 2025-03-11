from typing import Any

import pyarrow.dataset as ds
import s3fs
from dagster import AssetExecutionContext, Config, EnvVar
from dagster import AssetIn, Output, asset
from dagster_ncsa import S3ResourceNCSA
from deltalake import DeltaTable
from deltalake import write_deltalake

from dagster_socrata.socrata_resource import SocrataMetadata


class SocrataToDeltaLakeConfig(Config):
    min_row_group_size: int = 0  # Sets min rows per row group
    max_row_group_size: int = 100000  # Sets max rows per row group


@asset(
    ins={"socrata_metadata": AssetIn(), "socrata_to_object_store": AssetIn()},
    group_name="Socrata",
    name="socrata_to_deltalake",
    description="Converts Socrata dataset to Delta Lake",
)
def socrata_to_deltalake(
    context: AssetExecutionContext,
    config: SocrataToDeltaLakeConfig,
    socrata_to_object_store: dict[str, Any],
    socrata_metadata: SocrataMetadata,
    s3: S3ResourceNCSA,
) -> Output:

    bucket = EnvVar("DEST_BUCKET").get_value()
    table_path = f"delta/{socrata_to_object_store['socrata_domain']}/{socrata_metadata['id']}"  # NOQA E501
    delta_table_path = f"s3://{bucket}/{table_path}"  # NOQA E501
    s3.delete_directory(bucket, table_path)

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
