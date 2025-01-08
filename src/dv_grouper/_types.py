## Auxiliary/basic types which aren't part of the DVGrouper package data model
from typing import TypeAlias, TypeVar, Union

import polars as pl

## Custom polars types
polars_int: TypeAlias = Union[pl.Int8, pl.Int16, pl.Int32, pl.Int64]
polars_float: TypeAlias = Union[pl.Float32, pl.Float64]

SeriesFloat: TypeAlias = pl.Series
SeriesInt: TypeAlias = pl.Series
GenericMetadata = TypeVar("GenericMetadata")

DF: TypeAlias = Union[pl.DataFrame, pl.LazyFrame]
