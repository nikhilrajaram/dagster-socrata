from unittest.mock import MagicMock

from dagster import build_asset_context
from dagster_socrata.assets.metadata import socrata_metadata
from dagster_socrata_tests.conftest import MOCK_DATASET_ID, MockSocrataResource


def test_socrata_metadata_asset():
    # Create mock objects
    context = MagicMock()
    context.log = MagicMock()
    socrata_resource = MockSocrataResource()

    # Create a proper execution context with resources
    context = build_asset_context(
        resources={"socrata": socrata_resource},
        asset_config={"dataset_id": MOCK_DATASET_ID},
    )

    result = socrata_metadata(context=context)

    # Assertions
    assert result["name"] == "Table of Gross Cigarette Tax Revenue Per State (Orzechowski and Walker Tax Burden on Tobacco)"  # noqa: E501
    assert result["id"] == MOCK_DATASET_ID
