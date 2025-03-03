from dagster import Definitions, load_assets_from_modules, EnvVar, define_asset_job
from dagster_aws.s3 import S3Resource

from dagster_socrata import assets  # noqa: TID252
from dagster_socrata.resource import SocrataResource

all_assets = load_assets_from_modules([assets])
socrata_job = define_asset_job(
    name="DownloadSocrataDataset",
    selection=[assets.socrata_metadata,
               assets.socrata_to_object_store,
               assets.socrata_to_deltalake]
)

defs = Definitions(
    assets=all_assets,
    jobs=[socrata_job],
    resources={
        "socrata": SocrataResource(
            domain=EnvVar("SOCRATA_DOMAIN"),
            app_token=EnvVar("SOCRATA_APP_TOKEN")
        ),
        "s3": S3Resource(endpoint_url=EnvVar("S3_ENDPOINT_URL"),
                         aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
                         aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY")),
    }
)
