import os
from unittest.mock import MagicMock, patch

from dagster import ResourceDefinition, build_asset_context

from dagster_ncsa import S3ResourceNCSA
from dagster_socrata.assets import socrata_to_deltalake
from dagster_socrata_tests.conftest import MOCK_DATASET_ID, MockSocrataResource


@patch("dagster_socrata.assets.DeltaTable")
@patch("dagster_socrata.assets.write_deltalake")
@patch("dagster_socrata.assets.s3fs.S3FileSystem")
@patch("dagster_socrata.assets.ds.dataset")
def test_socrata_to_deltalake(
    mock_dataset, mock_s3fs, mock_deltalake, mock_delta_table
):
    # Create mock objects
    context = MagicMock()
    context.log = MagicMock()

    s3_mock = MagicMock(S3ResourceNCSA)
    s3_mock.client = MagicMock()
    s3_mock.aws_access_key_id = "test"
    s3_mock.aws_secret_access_key = "shhh"
    s3_mock.endpoint_url = "http://localhost:9000"
    s3_mock.list_files = MagicMock(
        return_value=[
            f"stage/{MOCK_DATASET_ID}/file1.csv",
            f"stage/{MOCK_DATASET_ID}/file2.csv",
        ],
    )  # noqa: E501
    s3_resource_definition = ResourceDefinition.hardcoded_resource(s3_mock)

    mock_fs = MagicMock()
    mock_s3fs.return_value = mock_fs
    os.environ["DEST_BUCKET"] = "test-bucket"

    socrata = MockSocrataResource()

    # Create a proper execution context with resources
    context = build_asset_context(
        resources={"s3": s3_resource_definition},
        asset_config={"min_row_group_size": 1000, "max_row_group_size": 2000},
    )
    socrata_to_object_store = {
        "socrata_domain": "data.cdc.gov",
        "csv_path": "stage/abcd-1234/",
    }
    result = socrata_to_deltalake(
        context=context,
        socrata_metadata=socrata.metadata,
        socrata_to_object_store=socrata_to_object_store,
    )

    # Assertions
    mock_s3fs.assert_called_with(
        key="test",
        secret="shhh",
        endpoint_url="http://localhost:9000",
    )

    s3_mock.list_files.assert_called_with("test-bucket", "stage/abcd-1234/", ".csv")

    mock_dataset.assert_called_with(
        [
            f"/test-bucket/stage/{MOCK_DATASET_ID}/file1.csv",
            f"/test-bucket/stage/{MOCK_DATASET_ID}/file2.csv",
        ],
        format="csv",
        filesystem=mock_fs,
        exclude_invalid_files=True,
        schema=socrata.metadata.schema,
    )

    storage_options = {
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "shhh",
        "AWS_ENDPOINT_URL": "http://localhost:9000",
    }

    mock_deltalake.assert_called_with(
        f"s3://test-bucket/delta/data.cdc.gov/{MOCK_DATASET_ID}",
        mock_dataset.return_value,
        name=socrata.metadata.table_name,
        description=socrata.metadata["name"],
        min_rows_per_group=1000,
        max_rows_per_group=2000,
        mode="overwrite",
        storage_options=storage_options,
        schema=socrata.metadata.schema,
    )

    mock_delta_table.assert_called_with(
        f"s3://test-bucket/delta/data.cdc.gov/{MOCK_DATASET_ID}",
        storage_options=storage_options,
    )

    mock_delta_table.return_value.create_checkpoint.assert_called()

    assert (
        result.value["delta_table_path"]
        == f"s3://test-bucket/delta/data.cdc.gov/{MOCK_DATASET_ID}"
    )
    assert result.value["table_name"] == "rkpp_igza"
