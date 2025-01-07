## Auxiliary/basic types which aren't part of the DVGrouper package data model
from typing import TypeAlias, Union

import polars as pl

## Custom polars types
polars_int: TypeAlias = Union[pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Int128]
polars_float: TypeAlias = Union[pl.Float32, pl.Float64]

SeriesFloat: TypeAlias = pl.Series[polars_float]
SeriesInt: TypeAlias = pl.Series[polars_int]
