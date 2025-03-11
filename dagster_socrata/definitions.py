from dagster import Definitions, load_assets_from_modules, EnvVar, define_asset_job
from dagster_ncsa import S3ResourceNCSA
from dagster_ncsa.airtable_catalog_resource import AirTableCatalogResource

from dagster_socrata import assets  # noqa: TID252
from dagster_socrata.socrata_resource import SocrataResource

all_assets = load_assets_from_modules([assets])
socrata_job = define_asset_job(
    name="DownloadSocrataDataset",
    selection=[
        assets.socrata_metadata,
        assets.socrata_to_object_store,
        assets.socrata_to_deltalake,
        assets.create_entry_in_data_catalog,
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
            api_key=EnvVar("AIRTABLE_API_KEY"),
            base_id=EnvVar("AIRTABLE_BASE_ID"),
            table_id=EnvVar("AIRTABLE_TABLE_ID"),
        ),
        "s3": S3ResourceNCSA(
            endpoint_url=EnvVar("S3_ENDPOINT_URL"),
            aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
        ),
    },
)
