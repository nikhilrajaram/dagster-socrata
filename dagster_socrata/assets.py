import json
from io import BytesIO

import pyarrow
from dagster import Config, EnvVar, asset, AssetExecutionContext
from dagster_aws.s3 import S3Resource
import pyarrow.parquet as pq

from dagster_socrata.resource import SocrataResource


class SocrataDatasetConfig(Config):
    dataset_id: str = "vdgb-f9s3"
    format: str = "json"
    chunk_size: int = 10000


@asset(
    compute_kind="socrata",
    group_name="data_ingestion"
)
def stream_socrata_to_s3(context: AssetExecutionContext,
                         config: SocrataDatasetConfig,
                         socrata: SocrataResource,
                         s3: S3Resource) -> None:
    """Asset that streams data from Socrata directly to S3"""
    bucket_name = EnvVar("DEST_BUCKET").get_value()
    context.log.info(f"Writing to {bucket_name}")

    s3_key = f"parquet/{socrata.domain}/{config.dataset_id}"
    s3_client = s3.get_client()

    with socrata.get_client() as client:
        metadata = client.get_metadata(config.dataset_id)
        s3_client.put_object(Bucket=bucket_name,
                             Key=f"{s3_key}/metadata.json",
                             Body=json.dumps(metadata))

        # Write DataFrame to parquet in memory
        partition = 0
        for json_records in client.get_dataset(
                config.dataset_id, limit=config.chunk_size):
            table = pyarrow.Table.from_pylist(json_records)
            context.log.info(f"Processing {table.num_rows} records into {bucket_name}")
            # Write DataFrame to parquet in memory
            buffer = BytesIO()
            pq.write_table(table, buffer, compression='snappy')
            buffer.seek(0)

            s3_client.upload_fileobj(
                buffer,
                bucket_name,
                s3_key + f"/part-{partition:03d}.parquet")
            partition += 1
