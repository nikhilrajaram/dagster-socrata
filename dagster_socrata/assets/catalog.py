from datetime import datetime

from dagster import AssetExecutionContext, Config
from dagster import AssetIn, asset
from dagster_ncsa.airtable_catalog_resource import AirTableCatalogResource
from dagster_ncsa.models import TableEntry


class CatalogConfig(Config):
    catalog: str = "PublicHealth"
    schema_name: str = "sdoh"


@asset(
    group_name="Socrata",
    name="create_entry_in_data_catalog",
    description="Create Entry in Data Catalog",
    ins={"socrata_metadata": AssetIn(), "socrata_to_deltalake": AssetIn()},
)
def create_entry_in_data_catalog(
    context: AssetExecutionContext,
    config: CatalogConfig,
    airtable: AirTableCatalogResource,
    socrata_metadata,
    socrata_to_deltalake,
):
    """
    Create an entry in the data catalog for the dataset.
    :param context:
    :param config:
    :param socrata_metadata:
    :param socrata_to_deltalake:
    :return:
    """
    table_entry = TableEntry(
        catalog=config.catalog,
        schema_name=config.schema_name,
        table=socrata_metadata.table_name,
        name=socrata_metadata["name"],
        description=socrata_metadata["description"],
        deltalake_path=socrata_to_deltalake["delta_table_path"],
        license_name=socrata_metadata.license,
        pub_date=datetime.fromtimestamp(socrata_metadata["publicationDate"]),
    )
    airtable.create_table_record(table_entry)
