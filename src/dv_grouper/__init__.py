"""
A package with two primary models for interacting with DataFrames and tabular (.parquet) data sources
"""

import logging
import os
import warnings
from collections.abc import Sequence
from datetime import datetime
from functools import wraps
from re import Pattern
from types import FunctionType
from typing import TYPE_CHECKING, Any, Callable, Literal, Mapping, Optional, Union

import pandera as pa
import polars as pl
from models import (
    BlobStorageUrl,
    LoadOptions,
    MarkdownOutput,
    MetadataOptions,
    NamedDataFrame,
    ObjectName,
    ParquetFile,
    SizeDesignator,
)
from pydantic import BaseModel, DirectoryPath, Field

from dv_grouper._types import DF, DataSource, GenericMetadata
from dv_grouper.models import DVBundleMetadata

CUR_DIR = os.path.dirname(os.path.abspath(__file__))
PAR_DIR = os.path.dirname(CUR_DIR)

logger = logging.getLogger(__name__)


class DVBundle(BaseModel):
    """
    Class for associating a DataFrame(s) with its validation schema, metadata, description, and functions.
    Think of a DVBundle as everything associated with a single data file and how you are using it in your program.

    TODO: Register the function which created the DataFrame for this DVBundle using a decorator.
    TODO: Register a transformation function to this DataFrame using a decorator (reads from function signature).
    TODO: Generate automated markdown documentation block for this bundle (its metadata, its related functions).
    """

    source: DataSource = Field(
        None,
        description="The data source associated with the DataFrame.",
        nullable=True,
        frozen=True,
    )

    df: DF = Field(
        None,
        description="A DataFrame/LazyFrame either loaded from `source` (if provided) or one already existing in memory; if None, .load() will read into a pl.LazyFrame",
        nullable=True,
        frozen=True,
    )

    schema: Optional[pa.DataFrameModel] = Field(
        None,
        description="The schema of the DataFrame represented as a DataFrameModel",
        frozen=True,
        nullable=True,
    )

    name: Optional[ObjectName] = Field(
        None,
        description="Descriptive name for the data. 1.) Must be valid python object name. 2.) If omitted, ObjectName must be obtainable from either `source` or `df`",
        frozen=True,
        nullable=True,
    )

    load_options: LoadOptions
    metadata_options: MetadataOptions

    @property
    def metadata(self) -> DVBundleMetadata:
        """
        Read the metadata attribute; does not re-scan the DataFrame for metadata.
        """
        if not hasattr(self, "__metadata"):
            self.__metadata = self.get_metadata(time_loaded=self._time_loaded)
        return self.__metadata

    def load(
        self,
    ) -> DF:
        """
        Loads and validates the data against the provided schema.
        Sets the self.df attribute to the loaded DataFrame/LazyFrame.
        """
        if self.df is None:
            if not self.load_options.reload_dataframe:
                warnings.warn(
                    f"DataFrame already loaded and reload_dataframe=={self.load_options.reload_dataframe}. Returning "
                )
                return self.df
            elif self.source is None:
                raise ValueError(f"Cannot load data if self.source is {self.source}.")
            else:
                # delattr(self, "df") TODO: check if needed with frozen=True
                df = self._load_df_from_data_source(
                    data_source=self.source,
                    load_lazy=self.load_options.load_lazy,
                    storage_options=self.load_options.storage_options,
                )
                self.df = df

        if self.schema:
            self.schema.validate(self.df)

        if self.load_options.metadata_on_load:
            opts = {}
            if self.load_options.add_timestamp:
                timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                opts["time_loaded"] = timestamp_str
            self.get_and_set_metadata(**opts)

        return self.df

    @classmethod
    def _load_df_from_data_source(
        cls,
        data_source: DataSource,
        load_lazy: Optional[bool] = None,
        storage_options: Optional[Mapping[str, Any]] = None,
    ) -> DF:
        df = None
        if ParquetFile.validate_ext(data_source) and ParquetFile.check_exists(
            data_source
        ):
            df = cls._load_parquet(data_to_read=data_source, load_lazy=load_lazy)
        elif BlobStorageUrl.validate(data_source):
            df = cls._load_parquet(
                data_to_read=data_source,
                load_lazy=load_lazy,
                storage_options=storage_options,
            )
        elif os.path.isdir(data_source):
            data_to_read = os.path.join(data_source, "*.parquet")
            df = cls._load_parquet(data_to_read=data_to_read, load_lazy=load_lazy)

        if df is None:
            raise TypeError(
                f"Failed to load data_source (type: {type(data_source)}): {data_source}"
            )
        else:
            return df

    @classmethod
    def _load_parquet(
        cls,
        data_to_read: Union[DataSource, str],
        load_lazy: Optional[bool] = None,
        storage_options: Optional[Mapping[str, Any]] = None,
    ) -> DF:
        """"""
        if load_lazy:
            df = pl.scan_parquet(data_to_read, storage_options=storage_options)
        else:
            df = pl.read_parquet(data_to_read, storage_options=storage_options)
        return df

    def get_and_set_metadata(
        self, **kwargs
    ) -> Union[DVBundleMetadata, GenericMetadata]:
        """
        Update the metadata dictionary for a DVBundle. Resets the self.__metadata attribute.
        Use **kwargs to add any additional tags.
        """
        if self.df is None:
            warnings.warn("self.df must be loaded before obtaining metadata.")
            return None
        else:
            metadata = self.metadata_options.metadata_function(self.df)
            metadata = {**metadata, **kwargs}
            self.__metadata = metadata
            return self.__metadata

    @wraps(DVBundleMetadata.register_func)
    def register_func(
        self,
        func: Callable,
        func_type: Optional[Literal["creates", "transforms", "references"]],
    ) -> None:
        DVBundleMetadata.register_func(func, func_type)

    @wraps(DVBundleMetadata.remove_func)
    def remove_func(
        self,
        func: Optional[Callable] = None,
    ) -> None:
        DVBundleMetadata.remove_func(func)


class DVGrouper(BaseModel):
    """
    Class for loading and validating groups of closely-related .parquet files into DVBundles (DataFrames with associated metadata).
    Dynamically assigns these data objects as named attributes for access via '.' notation.
    Can produce a unified set of MKDocs-compatible markdown documentation incorporating all data sources included in the collection.
    """

    data_sources: Sequence[Union[DataSource, DVBundle]] = Field(
        ...,
        description="""Collection of DataSources (ParquetFile, DirectoryPath, BlobStorageUrl, or pl.DataFrame) or DVBundles. 
                    Non-parquet files in a directory, as well as any nested subdirectories, will be ignored.""",
        frozen=True,
    )

    load_options: LoadOptions
    metadata_options: MetadataOptions

    def load(self: "DVGrouper") -> None:
        """
        Load data sources into DVBundles and dynamically assign them as attributes to the grouper.
        Assigns 'self.datasets' to the names of these bundles.

        Returns:
            datasets (tuple[str]): Tuple of dataset names in the DVGrouper
        """
        # Validate all inputs before loading
        error_reasons, n_errors = set(), 0
        for d in self.data:
            if not any(
                isinstance(d, t)
                for t in (ParquetFile, DirectoryPath, BlobStorageUrl, NamedDataFrame)
            ):
                error_reasons.add("All passed data must be a valid `DataSource`")
                n_errors += 1
                continue
            if isinstance(d, pl.DataFrame):
                if not NamedDataFrame.check(d):
                    error_reasons.add(
                        "If passing a DataFrame directly, it must have a valid `name` attribute."
                    )
            if self.load_options.require_schema and not (
                isinstance(d, DVBundle) and d.schema
            ):
                error_reasons.add(
                    "If `self.require_schema`, objects must all be DVBundles with `schema` attribute."
                )
            n_errors += 1
        if error_reasons:
            e = ValueError(f"{n_errors} invalid inputs for `data`: {error_reasons}")
            raise e

        # Load data into bundles and set as attributes
        datasets = []
        for data_source in self.data_sets:
            bundle = (
                data_source
                if isinstance(data_source, DVBundle)
                else DVBundle(
                    data_source=data_source,
                    metadata_function=self.metadata_options.metadata_function,
                    markdown_formatter=self.metadata_options.markdown_formatter,
                )
            )
            bundle.load(
                metadata_on_load=self.load_options.metadata_on_load,
                include_timestamp=self.load_options.add_timestamp,
                storage_options=self.load_options.storage_options,
            )
            setattr(self, bundle.name, bundle)
            datasets.append(bundle.name)
        datasets = tuple(datasets)
        self.datasets = datasets

        return

    def __repr__(self):
        """Print representation of the DVGrouper. Includes name of data set, years available, and (TO-DO: Data Set Description?)"""
        # Calculate the maximum length of the DataFrame names
        max_name_length = max(len(df_name) for df_name in self.metadata.keys())
        s = f"{'Dataset':<{max_name_length}} {'Years Available'}\n"
        s += f"{'--------':<{max_name_length}} {'-----------------'}\n"
        for df_name, m in sorted(self.metadata.items(), key=lambda x: x[0]):
            years = m["years_of_data"]
            s += f"{df_name:<{max_name_length}} ({', '.join(years)})\n"
        return s

    def search_cols(
        self,
        cols: Sequence[str | Pattern],
        use_regex: bool = False,
        # TODO: add option for regex flags
    ) -> dict[str, list]:
        """
        Search all the dataframes in a DVG for a given column name.

        Args:
            cols: Sequence of column names or regex patterns
            use_regex: Whether to interpret all strings in cols as regex patterns
        """
        result: dict[str, list] = {}
        for df_name in self.datasets:
            df: pl.DataFrame = getattr(self, df_name)
            matches = set()
            lookup_strs = []
            for c in cols:
                if isinstance(c, Pattern) or use_regex:
                    regex_matches = set(df.filter(regex=c).collect_schema().names)
                    matches.update(regex_matches)
                elif isinstance(c, str):
                    lookup_strs.append(c)
                else:
                    raise TypeError(
                        f"{c} must be `Pattern` or `str` (given: {type(c)})"
                    )
            if len(lookup_strs) > 0:
                exact_matches = set(df.filter(lookup_strs).columns)
                matches.update(exact_matches)
            if len(matches) > 0:
                result[df_name] = list(matches)
        return result

    @classmethod
    def to_markdown(
        cls, metadata: Sequence[GenericMetadata], *args: Any
    ) -> MarkdownOutput:
        """
        Take a sequence of metadata generated from individual DVBundles and create a unified MKDocs markdown document.
        """
        pass
