import os
from unittest.mock import MagicMock

from dagster_ncsa import S3ResourceNCSA
from dagster_socrata.assets.objectstore import socrata_to_object_store
from dagster_socrata_tests.conftest import MOCK_DATASET_ID, MockSocrataResource
from dagster import build_asset_context, ResourceDefinition


def test_socrata_to_object_store():
    # Create mock objects
    context = MagicMock()
    context.log = MagicMock()
    socrata_resource = MockSocrataResource(
        csv_data=[
            [["column1", "column2"], ["value1", "value2"]],
            [["column1", "column2"], ["value3", "value4"]],
        ]
    )

    socrata_resource.domain = "data.cdc.gov"
    os.environ["DEST_BUCKET"] = "test-bucket"

    s3_mock = MagicMock(S3ResourceNCSA)
    s3_mock.client = MagicMock()
    s3_resource_definition = ResourceDefinition.hardcoded_resource(s3_mock)

    # Create a proper execution context with resources
    context = build_asset_context(
        resources={"socrata": socrata_resource, "s3": s3_resource_definition},
        asset_config={"socrata_batch_size": 1000},
    )

    # Replace with your actual function
    result = socrata_to_object_store(
        context=context, socrata_metadata=socrata_resource.metadata
    )

    # Assertions
    s3_mock.delete_directory.assert_called_once_with(
        "test-bucket", f"stage/{MOCK_DATASET_ID}/"
    )
    s3_mock.save_json_object.assert_called_once_with(
        "test-bucket",
        f"stage/{MOCK_DATASET_ID}/metadata.json",
        socrata_resource.metadata.metadata,
    )
    socrata_resource.client.get_dataset.assert_called_once_with(
        MOCK_DATASET_ID, limit=1000
    )

    assert s3_mock.get_client.return_value.put_object.call_count == 2

    assert s3_mock.get_client.return_value.put_object.mock_calls[0][2] == {
        "Bucket": "test-bucket",
        "Key": f"stage/{MOCK_DATASET_ID}/PART-000.csv",
        "Body": "column1,column2\nvalue1,value2\n",
    }
    assert s3_mock.get_client.return_value.put_object.mock_calls[1][2] == {
        "Bucket": "test-bucket",
        "Key": f"stage/{MOCK_DATASET_ID}/PART-001.csv",
        "Body": "column1,column2\nvalue3,value4\n",
    }

    assert result.value["socrata_domain"] == "data.cdc.gov"
    assert result.value["csv_path"] == f"stage/{MOCK_DATASET_ID}/"

    assert result.metadata["dataset_id"].value == MOCK_DATASET_ID
