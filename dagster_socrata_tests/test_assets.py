
import dotenv
import pandas as pd
import pyarrow.dataset
import pytest
import s3fs
from dagster import EnvVar
from dagster_aws.s3 import S3Resource
from deltalake import DeltaTable, write_deltalake
from requests.exceptions import HTTPError

from dagster_socrata.assets import SocrataMetadata
from dagster_socrata.assets import list_csv_files
from dagster_socrata.resource import SocrataResource
from dagster_socrata.unity_catalog_resource import UnityCatalogResource


@pytest.fixture
def s3_resource() -> S3Resource:
    dotenv.load_dotenv("../.env")
    return S3Resource(endpoint_url=EnvVar("S3_ENDPOINT_URL").get_value(),
                      aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID").get_value(),
                      aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY").get_value())


def test_create_table(s3_resource):
    dotenv.load_dotenv("../.env")

    bucket_name = EnvVar("DEST_BUCKET").get_value()
    delta_path = f"s3://{bucket_name}/delta/data.cdc.gov/vdgb-f9s3/"
    storage_options = {"AWS_ACCESS_KEY_ID": s3_resource.aws_access_key_id,
                       "AWS_SECRET_ACCESS_KEY": s3_resource.aws_secret_access_key,
                       "AWS_ENDPOINT_URL": s3_resource.endpoint_url}

    unity = UnityCatalogResource(endpoint_url=EnvVar("UNITY_CATALOG_URL").get_value())
    unity.delete_table_if_exists("PublicHealth", "sdoh", "vdgb_f9s3")
    dt = DeltaTable(delta_path, storage_options=storage_options)
    print(dt.schema().to_json())
    try:
        result = unity.create_unity_catalog_table(
            dt=dt,
            name='vdgb_f9s3',
            catalog_name='PublicHealth',
            schema_name='sdoh',
            table_type='EXTERNAL',
            data_source_format='DELTA',
            storage_location=delta_path,
            comment=None,
            properties=None
        )
    except HTTPError as e:
        print(e.response.content)
        assert False
    print(result)


@pytest.mark.skip(reason="Different focus")
def test_deltalake_table(s3_resource):
    bucket_name = EnvVar("DEST_BUCKET").get_value()
    fs = s3fs.S3FileSystem(key=EnvVar('AWS_ACCESS_KEY_ID').get_value(),
                           secret=EnvVar('AWS_SECRET_ACCESS_KEY').get_value(),
                           endpoint_url=EnvVar('S3_ENDPOINT_URL').get_value())

    parquet_files = [f"/{bucket_name}/{file}"
                     for file in
                     list_csv_files(bucket_name, 'stage/vdgb-f9s3/', s3_resource)]

    metadata = SocrataMetadata.read(s3_resource.get_client(),
                                    bucket_name, "stage/vdgb-f9s3/metadata.json")

    parquet_dataset = pyarrow.dataset.dataset(parquet_files,
                                              format="csv",
                                              filesystem=fs,
                                              exclude_invalid_files=True,
                                              schema=metadata.schema)

    # Write the dataset to Delta format
    delta_path = f"s3://{bucket_name}/delta/data.cdc.gov/vdgb-f9s3"
    storage_options = {"AWS_ACCESS_KEY_ID": s3_resource.aws_access_key_id,
                       "AWS_SECRET_ACCESS_KEY": s3_resource.aws_secret_access_key,
                       "AWS_ENDPOINT_URL": s3_resource.endpoint_url}

    write_deltalake(delta_path, parquet_dataset, mode="overwrite",
                    storage_options=storage_options,
                    schema=metadata.schema)

    delta_table = DeltaTable(delta_path, storage_options=storage_options)
    print(delta_table)


@pytest.mark.skip(reason="Different focus")
def test_metadata_to_schema():
    dotenv.load_dotenv("../.env")
    socrata = SocrataResource(domain=EnvVar('SOCRATA_DOMAIN').get_value(),
                              app_token=EnvVar('SOCRATA_APP_TOKEN').get_value(),
                              timeout=60)
    with socrata.get_client() as client:
        metadata = client.get_metadata("vdgb-f9s3")
        m = SocrataMetadata(metadata)
        print(m.casts)


@pytest.mark.skip(reason="Different focus")
def test_csv():
    dotenv.load_dotenv("../.env")
    socrata = SocrataResource(domain=EnvVar('SOCRATA_DOMAIN').get_value(),
                              app_token=EnvVar('SOCRATA_APP_TOKEN').get_value(),
                              timeout=60)
    with socrata.get_client() as client:
        count = 0
        for data in client.get_dataset("9hdi-ekmb", limit=10, content_type="csv"):
            # Extract the headers (first row)
            headers = data[0]

            # Extract the data (remaining rows)
            rows = data[1:]

            # Create the DataFrame
            df = pd.DataFrame(rows, columns=headers)
            print(df)

            count += 1
            if count > 10:
                break
