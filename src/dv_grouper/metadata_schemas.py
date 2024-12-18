## Pydantic models for input validation of metadata schemas
# (End users probably won't be instantiating these directly
import logging
import warnings
from collections.abc import Collection
from datetime import datetime
from types import FunctionType
from typing import (
    Callable,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Union,
    Any
)

import polars as pl
from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    DirectoryPath,
    Field,
    FilePath,
)

logger = logging.getLogger(__name__)


from _types import DataFrameModel, ObjectName, MarkdownOutput, SizeDesignator

class ColumnMetadata(BaseModel):
    """
    Data Model for the metadata on the columns of a DataFrame Column.
    """

    model_config = ConfigDict(
        frozen=True, 
        arbitrary_types_allowed=True
    )  # Deliberately create a new object rather than changing anything here

    name: str
    dtype: pl.DataType
    size: str # "SizeDesignator"
    example_values: tuple[Any] # pl.DataType
    n_rows: int
    n_unique: int
    n_null: int
    description: Optional[str] = None
    range_values: Optional[Sequence[Any]] = Field( # pl.DataType
        default=None, description="Range of values for e.g. a date column"
    )


class DVBundleExternalMetadata(BaseModel):
    """
    Manually-added/"external" metadata for a DataFrame (i.e. not tied strictly to the DataFrame at a given time)
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: ObjectName = Field(
        ...,
        description="Descriptive name for the DataFrame associated with this DVBundle.",
        frozen=True,
    )
    orig_source: Optional[Union[FilePath, DirectoryPath]] = Field(
        None,
        description="File path (or URL) to the DataFrame source data; this might be omitted if the DVBundle loaded a DataFrame directly",
        frozen=True,
    )
    schema: DataFrameModel = Field(
        ...,
        description="DataFrameModel to validate DataFrame against when loading data or assigning an existing DataFrame",
        frozen=True,
    )
    description: Optional[str] = Field(
        None,
        description="Any other notes or description on the data source",
        frozen=True,
    )
    links: Optional[Collection[AnyUrl]] = Field(
        None, description="Any other weblinks related to the data source", frozen=True
    )
    functions: Optional[Collection[FunctionType]] = Field(
        None,
        description="Any functions related to the DataFrame; can be added to or removed from via decorators in dv_grouper.decorators.py",
        frozen=False,
    )
    time_loaded: Optional[datetime] = Field(
        None, description="Date the ", frozen=True
    )  # re-create DVBundleExternalMetadata on load


class DVBundleMetadata(DVBundleExternalMetadata):
    """
    Model for metadata on an entire DataFrame. Return type for the default DVBundle.get_metadata() method.
        NOTE: Users can define and pass in their own metadata functions when creating a DVBundle. This model will then not be applicable.
    """

    ## Metadata about the DataFrame as it exists in memory
    size: "SizeDesignator"
    shape: tuple[int, int]
    n_null: int = Field(..., description="Number of rows with all null columns")
    n_duplicates: int = Field(..., description="Number of duplicate rows")

    column_metadata: Mapping[str, "ColumnMetadata"]

    @property
    def n_null_by_column(self) -> Mapping[str, int]:
        """TODO Return mapping of number of nulls per column"""
        pass

    @property
    def n_unique_by_column(self) -> Mapping[str, int]:
        """TODO Return mapping of number of unique values per column"""
        pass

    @classmethod
    def from_df(
        cls,
        df: pl.DataFrame,
        n_unique_include: int = 10,
        size_unit: Literal["mb", "kb", "b"] = "b",
        column_descriptions: Optional[Mapping[str, str]] = None,
        df_options: Optional[DVBundleExternalMetadata] = None,
    ) -> "DVBundleMetadata":
        """
        Create a DVBundleMetadata instance from a Polars DataFrame.

        Args:
            df (pl.DataFrame): The DataFrame from which to create the DVBundleMetadata.
            n_unique_include (int, optional): The number of unique values to include for metadata analysis. Defaults to 10.
            size_unit (Literal['mb', 'kb', 'b'], optional): The unit of size to use for the metadata. Defaults to 'b' (bytes).
            column_descriptions (Mapping, optional): Additional options for column-specific configurations.
            df_options (Mapping, optional): Additional options for DataFrame-specific configurations.

        Returns:
            DVBundleMetadata: A new instance of DVBundleMetadata created from the DataFrame.

        """

        column_descriptions = column_descriptions or {}
        df_options = df_options or {}

        ## TO-DO: Which is faster? This or the individual DataFrame?
        # df_counts = df.select(
        #     *(pl.col(col).alias(f'{col}_n_unique').approx_n_unique() for col in df.columns),
        #     *(pl.col(col).len().alias(f'{col}_n_total') for col in df.columns)
        #     *(pl.col(col).null_count().alias(f'{col}_n_total') for col in df.columns)
        # ).to_dict(as_series=False)

        df_count_unique: pl.DataFrame = df.select(pl.all().approx_n_unique())
        df_count_total: pl.DataFrame = df.select(pl.all().len())
        df_count_null: pl.DataFrame = df.select(pl.all().null_count())

        col_types = dict(df.schema)

        column_metadata = {
            col: ColumnMetadata(
                {
                    "name": col,
                    "dtype": col_types[col],
                    "size": SizeDesignator.create_str(
                        n=df.select(col).estimated_size(size_unit), unit=size_unit
                    ),
                    "example_values": df[col].unique().head(n_unique_include).to_list(),
                    "n_rows": df_count_total.select(col).to_series().to_list(),
                    "n_unique": df_count_unique.select(col).to_series().to_list(),
                    "n_null": df_count_null.select(col).to_series().to_list(),
                    "description": column_descriptions.get(col, None),
                }
            )
            for col in df.columns
        }

        df_metadata = DVBundleMetadata(
            {
                **{
                    "size": f"{df.estimated_size(unit=size_unit)} {size_unit.upper()}",
                    "shape": df.shape,
                    "n_null": sum((c["n_null"] for c in column_metadata)),
                    "n_duplicates": df.is_duplicated().len(),
                    "columns": column_metadata,
                },
                **df_options,
            }
        )

        return df_metadata

    def register_func(
        self,
        func: Callable,
        func_type: Optional[Literal["creates", "transforms", "references"]],
    ) -> None:
        """
        TODO: Register a function as being associated with the DVBundle/DataFrame.
        Wrapped by funcs.decorators.register_func().
        Wrapper for types.FunctionRegistry.register_func().

        Logs warning if function is already registered.
        """

        if func in set(self.functions):
            warnings.warn(f"Function {func.__name__} already in registered functions.")
            return
        else:
            setattr(func, 'func_type', func_type)
            self.functions.append(func)

    def remove_func(
        self,
        func: Optional[Callable] = None,
    ) -> None:
        """
        TODO: Remove a function from the registry (either give func itself or its name)

        Logs warning if function is not already registered and you attempt to remove it.
        """
        pass

    @classmethod
    def to_markdown(cls, metadata: "DVBundleMetadata") -> MarkdownOutput:
        """
        TODO: create a block of markdown text.
        """
        pass
