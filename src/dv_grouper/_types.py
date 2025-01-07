## Auxiliary/basic types which aren't part of the DVGrouper package data model
from typing import TYPE_CHECKING, TypeAlias, TypeVar, Union

import polars as pl

## Custom polars types
polars_int: TypeAlias = Union[pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Int128]
polars_float: TypeAlias = Union[pl.Float32, pl.Float64]

SeriesFloat: TypeAlias = pl.Series[polars_float]
SeriesInt: TypeAlias = pl.Series[polars_int]
GenericMetadata = TypeVar("GenericMetadata")

DF: TypeAlias = Union[pl.DataFrame, pl.LazyFrame]

if TYPE_CHECKING:
    from models import BlobStorageUrl, DirectoryPath, ParquetFile

    DataSource: TypeAlias = Union[ParquetFile, DirectoryPath, BlobStorageUrl]
