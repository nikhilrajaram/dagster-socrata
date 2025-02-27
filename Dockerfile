FROM python:3.12
WORKDIR /project
COPY pyproject.toml /project/

RUN pip install .
COPY dagster_socrata/ /project/dagster_socrata/
RUN pip install -e .
