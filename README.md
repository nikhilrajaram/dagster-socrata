# Dagster Socrata

Dagster Project to download datasets from [Socrata portals](https://dev.socrata.com/). Socrata is a popular open data 
platform used by many cities and organizations to share data with the public. It exposes a REST API that can be used to
query and download datasets.

The purpose of this project is to provide a Dagster Resource for interfacing with the Socrata API that can download 
datasets and metadata specifications from Socrata portals and store them in a cloud bucket. The project includes assets
to use this resource in a full cloud data lake pipeline.

## The SocrataResource
The SocrataResource can be initialized in your definitions with the following code:

```python
from dagster_socrata.socrata_resource import SocrataResource
from dagster import EnvVar

resources={
    "socrata": SocrataResource(
        domain=EnvVar("SOCRATA_DOMAIN"), app_token=EnvVar("SOCRATA_APP_TOKEN")
    )
}
```

The domain should look like `data.cdc.gov`. The app token is a token that you can get from the Socrata portal. The
resource accepts an optional `timeout` parameter that can be used to set the timeout for the requests to the 
Socrata API.

## Using the SocrataResource
The SocrataResource provides a context manager that vends a wrapped Socrata client. The wrapper provides convinent
ways to access the core Socrata functionality:

### Obtaining Metadata for a Dataset
```python
with context.resources.socrata as socrata:
    metadata = socrata.get_metadata("jqwm-z2g9")
```

This method returns a Metadata object that contains the metadata for the dataset. The SocrataMetadata object has 
getters for accessing data in the metadata dictionary. It also offers convenience methods for saving the 
metadata as a json object in a bucket, as well as reading that data back. It can also generate a pyarrow schema
for use in the data lake.

### Downloading a Dataset
The client wrapper has a `get_dataset` method that acts as a generator, returning the rows of the dataset in 
chunks. These chunks represent calls to the Socrata `get` method using the `limit` parameter to determine
how many rows at a time to request. The method uses pagination to return the entire dataset over calls to the
generator.

The method accepts the following parameters:
- `dataset_id` - The id of the dataset to download
- `limit` - The number of rows to request at a time
- `format` - The format of the data. Can be 'json' or 'csv'

## Assets
In order to create a full pipeline, the project includes assets that use the SocrataResource to download datasets
and build a cloud data lake.

### Socrata Metadata
This asset uses the SocrataResource to download the metadata for a dataset and provide it as a result for use 
in downstream assets. The asset uses the `SocrataDatasetConfig` as its configuration. In this configuration, you
can specify the `dataset_id`.

### Socrata to Object Store
This asset accepts the dataset ID and other metadata and downloads the dataset to a cloud bucket. The asset uses
a configured S3ResourceNCSA to store the data in the bucket. The asset uses the `SocrataAPIConfig` where the 
chunk size can be specified.

The asset uses the `DEST_BUCKET` environment variable to determine the bucket to store the data in. It saves
the downloaded csv file to a path: `"stage/{dataset_id}/`. It also saves a copy of the metadata to the same
bucket in a path: `metadata/{dataset_id}/metadata.json`.

### Socrata to Data Lake
In order to make the data more easily queryable, the asset converts the csv file to a parquet file and stores
DeltaLake schema data in the bucket. The asset uses the `SocrataDataLakeConfig` to configure how the parquet 
files are structured. The resulting DeltaTable is stored in the `DEST_BUCKET` in a 
path: `delta/{socrata_domain}/{dataset_id}`.

### Data Catalog
At NCSA, we use a simple AirTable database to store metadata about the datasets in the data lake. We intend to replace
this with UnityCatalog as soon as bugs are worked out for data stored in non-AWS buckets. The 
`create_entry_in_data_catalog` asset uses the catalog interface from the dagster-ncsa package to create an entry in the
AirTable database. The asset uses the `DataCatalogConfig` to configure the connection to the AirTable database.

This configuration allows you to specify the _catalog_ and _schema name_ for the table. The table name is assumed
to be the same as the dataset ID.

## Getting started
First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.


## Development

### Adding new Python dependencies

You can specify new Python dependencies in `pyproject.toml`.

### Unit testing

Tests are in the `dagster_socrata_tests` directory and you can run tests using `pytest`:

```bash
pytest dagster_socrata_tests
```

# Using
Accessing DeltaTable from DuckDB

```sql
INSTALL httpfs;
LOAD httpfs;

-- Load the Delta Lake extension
INSTALL delta;
LOAD delta;

-- Create an S3 secret for your object store configuration
CREATE SECRET osn_rice (
    TYPE S3,
    ENDPOINT 'rice1.osn.mghpcc.org',
    USE_SSL true,
    URL_STYLE path
);

-- Now you can query your Delta table
-- Replace 'bucket-name/path/to/delta-table' with your actual path
SELECT * FROM delta_scan('s3://sdoh-public/delta/data.cdc.gov/jqwm-z2g9');

```
