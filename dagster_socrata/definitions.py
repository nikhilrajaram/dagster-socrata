from dagster import Definitions, load_assets_from_modules, EnvVar
from dagster_aws.s3 import S3Resource

from dagster_socrata import assets  # noqa: TID252
from dagster_socrata.resource import SocrataResource

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
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
