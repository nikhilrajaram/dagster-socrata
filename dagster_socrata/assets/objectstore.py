import io

import pandas as pd
from dagster import AssetIn, Output, asset
from dagster import Config, EnvVar
from dagster_ncsa import S3ResourceNCSA

from dagster_socrata.socrata_resource import SocrataResource


class SocrataAPIConfig(Config):
    socrata_batch_size: int = 2000


@asset(
    ins={"socrata_metadata": AssetIn()},
    group_name="Socrata",
    name="socrata_to_object_store",
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
