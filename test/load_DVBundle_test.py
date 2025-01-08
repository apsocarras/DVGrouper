import logging

import pandera as pa
import polars as pl
import pytest
from faker import Faker

from dv_grouper import DVBundle
from dv_grouper.models import LoadOptions, MetadataOptions, ParquetFile


class MyDataModel(pa.DataFrameModel):
    id: int
    name: str
    age: int


logger = logging.getLogger(__name__)


@pytest.fixture
def load_no_metadata() -> LoadOptions:
    opts = LoadOptions(
        metadata_on_load=False,
    )
    return opts


@pytest.fixture
def load_metadata_not_lazy() -> LoadOptions:
    opts = LoadOptions(
        metadata_on_load=True,
        load_lazy=True,
    )
    return opts


@pytest.fixture
def faker_instance() -> Faker:
    return Faker()


@pytest.fixture
def example_metadata_options(faker_instance) -> MetadataOptions:
    opts = MetadataOptions(
        description="Example metadata options",
        links=[faker_instance.url() for _ in range(3)],
    )

    return opts


@pytest.fixture
def example_df() -> pl.DataFrame:
    data = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    df = pl.DataFrame(data)
    return df


@pytest.fixture
def example_parquet(example_df, tmp_path):
    parquet_path = tmp_path / "example.parquet"
    example_df: pl.DataFrame
    example_df.write_parquet(parquet_path)
    return parquet_path


@pytest.mark.parametrize(
    "parquet_file, df, model, name",
    (
        (
            "example_parquet",
            None,
            MyDataModel,
            "ex_name",
        ),
        (
            None,
            "example_df",
            MyDataModel,
            "ex_name",
        ),
        (
            "example_parquet",
            "example_df",
            MyDataModel,
            "ex_name",
        ),
    ),
)
def test_load_no_metadata(
    parquet_file, df, model, name, request, example_metadata_options, load_no_metadata
):
    parquet_file = (
        request.getfixturevalue(parquet_file) if parquet_file is not None else None
    )
    df = request.getfixturevalue(df) if df is not None else None

    if parquet_file is not None:
        logger.debug(
            f"Attempting to parse parquet file path {parquet_file} to object (type: {type(parquet_file)})"
        )
        parquet_parsed = ParquetFile(file_path=parquet_file)
    else:
        parquet_parsed = parquet_file

    dvb = DVBundle(
        source=parquet_parsed,
        df=df,
        model=model,
        name=name,
        metadata_options=example_metadata_options,
        load_options=load_no_metadata,
    )
    dvb.load()

    # Test functionality of loaded dvb
    dvb.name
