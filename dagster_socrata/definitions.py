import os

from dagster import Definitions, EnvVar, define_asset_job
from dagster_ncsa import S3ResourceNCSA
from dagster_ncsa.airtable_catalog_resource import AirTableCatalogResource
import dagster_socrata.assets   # noqa: TID252
from dagster_socrata.socrata_resource import SocrataResource

all_assets = [
    dagster_socrata.assets.metadata.socrata_metadata,
    dagster_socrata.assets.objectstore.socrata_to_object_store,
    dagster_socrata.assets.deltatable.socrata_to_deltalake,
    dagster_socrata.assets.catalog.create_entry_in_data_catalog,
]

socrata_job = define_asset_job(
    name="DownloadSocrataDataset",
    selection=[
        dagster_socrata.assets.metadata.socrata_metadata,
        dagster_socrata.assets.objectstore.socrata_to_object_store,
        dagster_socrata.assets.deltatable.socrata_to_deltalake,
        dagster_socrata.assets.catalog.create_entry_in_data_catalog,
    ],
)

defs = Definitions(
    assets=all_assets,
    jobs=[socrata_job],
    resources={
        "socrata": SocrataResource(
            domain=EnvVar("SOCRATA_DOMAIN"), app_token=EnvVar("SOCRATA_APP_TOKEN")
        ),
        "airtable": AirTableCatalogResource(
            api_key=os.environ["AIRTABLE_API_KEY"],
            base_id=os.environ["AIRTABLE_BASE_ID"],
            table_id=os.environ["AIRTABLE_TABLE_ID"]
        ),
        "s3": S3ResourceNCSA(
            endpoint_url=EnvVar("S3_ENDPOINT_URL"),
            aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
        ),
    },
)
