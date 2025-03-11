import dotenv
import pytest

from dagster_ncsa import S3ResourceNCSA
from dagster import EnvVar

from dagster_socrata.socrata_resource import SocrataResource


@pytest.fixture
def s3_resource() -> S3ResourceNCSA:
    dotenv.load_dotenv("../../.env")
    return S3ResourceNCSA(
        endpoint_url=EnvVar("S3_ENDPOINT_URL").get_value(),
        aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID").get_value(),
        aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY").get_value(),
    )


@pytest.fixture
def socrata_resource():
    dotenv.load_dotenv("../../.env")
    return SocrataResource(
        domain=EnvVar("SOCRATA_DOMAIN").get_value(),
        app_token=EnvVar("SOCRATA_APP_TOKEN").get_value(),
    )
