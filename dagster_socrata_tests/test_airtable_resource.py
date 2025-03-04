from datetime import datetime

import dotenv
from dagster import EnvVar
from dagster_socrata.airtable_resource import AirTableResource
from dagster_socrata.assets import SocrataMetadata


def test_create_table(s3_resource):
    dotenv.load_dotenv("../.env")

    bucket_name = "sdoh-public"
    delta_path = f"s3://{bucket_name}/delta/data.cdc.gov/vdgb-f9s3/"
    airtable = AirTableResource(
        api_key=EnvVar("AIRTABLE_API_KEY").get_value(),
        base_id=EnvVar("AIRTABLE_BASE_ID").get_value(),
        table_id=EnvVar("AIRTABLE_TABLE_ID").get_value())

    metadata = SocrataMetadata.read(s3_resource.get_client(),
                                    bucket_name, "stage/9bhg-hcku/metadata.json")
    airtable.create_table_record(
        catalog="PublicHealth",
        schema="sdoh",
        table="vdgb_f9s3",
        description=metadata['description'],
        deltalake_path=delta_path,
        license=metadata['license']['name'],
        pub_date=datetime.fromtimestamp(metadata['publicationDate'])
    )
