from datetime import datetime
from unittest.mock import MagicMock
from dagster import build_asset_context, ResourceDefinition

from dagster_ncsa.airtable_catalog_resource import AirTableCatalogResource
from dagster_socrata.assets.catalog import create_entry_in_data_catalog
from dagster_socrata_tests.conftest import MockSocrataResource


def test_create_entry_in_data_catalog():
    # Create mock objects
    context = MagicMock()
    context.log = MagicMock()

    socrata = MockSocrataResource()
    socrata_to_deltalake = {
        "delta_table_path": "s3://test-bucket/delta-table",
    }

    mock_airtable_resource = MagicMock(AirTableCatalogResource)
    # Create a proper execution context with resources
    context = build_asset_context(
        resources={
            "airtable": ResourceDefinition.hardcoded_resource(mock_airtable_resource)
        },
        asset_config={"catalog": "PublicHealth", "schema_name": "sdoh"},
    )

    create_entry_in_data_catalog(
        context,
        socrata_metadata=socrata.metadata,
        socrata_to_deltalake=socrata_to_deltalake,
    )

    # Assertions
    mock_airtable_resource.create_table_record.assert_called_once_with(
        catalog="PublicHealth",
        schema="sdoh",
        table=socrata.metadata.table_name,
        name=socrata.metadata["name"],
        description=socrata.metadata["description"],
        deltalake_path=socrata_to_deltalake["delta_table_path"],
        license_name=socrata.metadata.license,
        pub_date=datetime(2021, 3, 22, 4, 49, 27),
    )
