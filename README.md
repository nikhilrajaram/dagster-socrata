# dagster_socrata

Dagster Project to download datasets from Socrata portals.
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

You can start writing assets in `dagster_socrata/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `dagster_socrata_tests` directory and you can run tests using `pytest`:

```bash
pytest dagster_socrata_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/guides/automate/schedules/) or [Sensors](https://docs.dagster.io/guides/automate/sensors/) for your jobs, the [Dagster Daemon](https://docs.dagster.io/guides/deploy/execution/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.


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
