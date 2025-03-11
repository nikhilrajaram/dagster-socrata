from dagster import AssetExecutionContext, Config, asset

from dagster_socrata.socrata_resource import SocrataMetadata, SocrataResource


class SocrataDatasetConfig(Config):
    dataset_id: str = "vdgb-f9s3"


@asset(
    group_name="Socrata",
    name="socrata_metadata",
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
