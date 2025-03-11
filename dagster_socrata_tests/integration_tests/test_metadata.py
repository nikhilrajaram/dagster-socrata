import json
import pytest
from dagster_socrata.socrata_resource import SocrataMetadata
from dagster import EnvVar


@pytest.mark.parametrize("dataset_id", ["rkpp-igza"])
def test_read_from_s3(s3_resource, dataset_id):
    metadata = SocrataMetadata.read(
        s3_resource,
        EnvVar("DEST_BUCKET").get_value(),
        f"stage/{dataset_id}/metadata.json",
    )

    assert metadata["id"] == dataset_id
    print(json.dumps(metadata, indent=2))


@pytest.mark.parametrize("dataset_id", ["rkpp-igza"])
def test_read_from_socrata(socrata_resource, dataset_id):
    with socrata_resource.get_client() as client:
        metadata = client.get_metadata(dataset_id)

    assert metadata["id"] == dataset_id
    print(metadata)


@pytest.mark.parametrize("dataset_id", ["rkpp-igza"])
def test_write(socrata_resource, s3_resource, dataset_id):
    with socrata_resource.get_client() as client:
        metadata = client.get_metadata(dataset_id)

    assert metadata["id"] == dataset_id
    metadata.save(EnvVar("DEST_BUCKET").get_value(), "test/metadata.json", s3_resource)
    print(metadata)
