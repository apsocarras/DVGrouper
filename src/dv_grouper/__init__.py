import polars as pl

import os

from functools import wraps

from typing import Literal, Union, Optional, Mapping, Callable, Any, TypeVar, TypeAlias
from abc import ABC
from types import FunctionType
from collections.abc import Sequence
from metadata_schemas import DVBundleMetadata
from pydantic import (
    Field,
    BaseModel,
    DirectoryPath,
    AnyUrl,
    ConfigDict,
)
from re import Pattern
from datetime import datetime
from pathlib import Path
import logging

from _types import (
    SizeDesignator,
    DataSource,
    DataFrameModel,
    ParquetFile,
    ObjectName,
    MarkdownOutput,
    BlobStorageUrl,
    NamedDataFrame,
)

CUR_DIR = os.path.dirname(os.path.abspath(__file__))
PAR_DIR = os.path.dirname(CUR_DIR)

logger = logging.getLogger(__name__)


## TypeVar: Generic placeholder for metadata on a DataFrame
GenericMetadata = TypeVar("GenericMetadata")


class _DataCollection(BaseModel):
    model_config = ConfigDict(extra="allow")

    description: Optional[str] = Field(
        None,
        description="An optional description of the object's data source(s).",
        frozen=False,
    )

    links: Optional[Sequence[AnyUrl]] = Field(
        None, description="An optional list of URLs related to the object", frozen=False
    )

    functions: Optional[Sequence[FunctionType]] = Field(
        None,
        description="An optional collection of functions related to the object. Functions can be added to this collection via @tag_func()",
        frozen=False,
    )

    size_unit: SizeDesignator = Field(
        "mb", description="The size unit of the object (default is 'mb')", frozen=False
    )

    metadata_on_load: Optional[bool] = Field(
        False,
        description="Whether to load metadata on along with data (default is False)",
        frozen=False,
    )

    metadata_function: Optional[
        Callable[[pl.DataFrame, *tuple[Any, ...]], GenericMetadata]
    ] = Field(
        default=DVBundleMetadata.from_df,
        description="""Function for obtaining metadata from data sources in `data`. 
                    Must be consistent with the markdown formatter function, if provided.
                    Default is DVBundleMetadata.from_df().""",
        frozen=False,
    )

    markdown_formatter: Optional[
        Callable[[GenericMetadata, *tuple[Any, ...]], MarkdownOutput]
    ] = Field(
        default=DVBundleMetadata.to_markdown,
        description="""An optional function to format metadata into markdown output. 
        Must be consistent with the output of your metadata function. 
        Requires metadata function.""",
        frozen=False,
    )

    include_timestamp: Optional[bool] = Field(
        None, description="Whether to include a timestamp in the data collection."
    )

    _time_loaded: Optional[datetime] = (
        None  # Internal field to mark when data was last read
    )

    storage_options: Optional[Mapping[str, Any]] = Field(
        None,
        description="""A dictionary of credentials (e.g., API keys) required for data access. Defaults to None. See polars documentation for valid options.
        Caution: You may want to use this on load() rather than persist them along with the object.
        """,
    )

    load_lazy: Optional[bool] = Field(
        True, description="Whether to load in DataFrames lazily or eagerly."
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)


class DVBundle(_DataCollection):
    """
    Class for associating a DataFrame(s) with its validation schema, metadata, description, and functions.
    Think of a DVBundle as everything associated with a single data file and how you are using it in your program.

    TODO: Register the function which created the DataFrame for this DVBundle using a decorator.
    TODO: Register a transformation function to this DataFrame using a decorator (reads from function signature).
    TODO: Generate automated markdown documentation block for this bundle (its metadata, its related functions).
    """

    data_source: DataSource = Field(
        ...,
        description="The data source associated with the DataFrame OR the DataFrame itself; for most use cases, this should probably point to something in storage and not already in memory.",
        frozen=True,
    )
    schema: Optional[DataFrameModel] = Field(
        None,
        description="The schema of the DataFrame represented as a DataFrameModel",
        frozen=True,
    )

    name_: Optional[ObjectName] = Field(
        None,
        description="Descriptive name for the data source. 1.) Must be valid python object name. 2.) If omitted, ObjectName must be obtainable from `data_source`",
        frozen=True,
    )
    _data: Optional[pl.DataFrame] = None  # Internal field to store loaded data

    @property
    def name(self) -> ObjectName:
        """
        Descriptive name for the DataFrame; default is the name of the file or directory in data_source
        """
        return self.name_ or ObjectName.from_data_source(self.data_source)

    @property
    def data(self) -> pl.DataFrame:
        """
        A property to access the loaded data.
        If `_data` is None, defaults to `data_source`.
            NOTE:
                - on DVBundle.load(), `_data` is assigned the DataFrame loaded from `data_source`
                - `_data` should be none if `data_source` is already a DataFrame.
        """
        return self._data or self.data_source

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
        load_lazy: Optional[bool] = None,
        metadata_on_load: Optional[bool] = None,
        include_timestamp: Optional[bool] = True,
        storage_options: Optional[Mapping[str, Any]] = None,
    ) -> pl.DataFrame:
        """
        Loads and validates the data against the provided schema.

        Args:
            load_lazy: (Optional[bool]): Whether to load in polars DataFrame in lazy or eager mode.
            metadata_on_load (Optional[bool]): If True, the metadata is extracted and stored alongside the data.
                - This attribute can be accessed/updated later using the `self.get_metadata()` method.
            include_timestamp (Optional[bool]): If True, a timestamp is added to `self.date_loaded`
                                                indicating when the data was loaded.
            storage_options (Optional[Mapping[str, str]]): A dictionary of credentials (e.g., API keys)
                                                    required for data access. Defaults to None. See polars documentation for valid options.
        Returns:
            pl.DataFrame: The loaded data as a Polars DataFrame.
        """

        if self.schema:
            self.schema.validate(df)

        if include_timestamp:
            timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._time_loaded = timestamp_str

        if metadata_on_load or (
            metadata_on_load is None and self.metadata_on_load
        ):  # can overwrite if set
            self.get_metadata(time_loaded=self._time_loaded)

        self._data = df

        return self._data

    @classmethod
    def _df_from_data_source(
        cls,
        data_source: DataSource,
        load_lazy: Optional[bool] = None,
        storage_options: Optional[Mapping[str, Any]] = None,
    ) -> pl.DataFrame:
        match data_source:
            case pl.DataFrame():
                df = data_source
            case _ if ParquetFile.validate(data_source):
                df = cls._check_lazy_load(data_to_read=data_source, load_lazy=load_lazy)
            case _ if BlobStorageUrl.validate(data_source):
                df = cls._check_lazy_load(
                    data_to_read=data_source,
                    load_lazy=load_lazy,
                    storage_options=storage_options,
                )
            case _ if os.path.basename(data_source) == "":  # directory
                data_to_read = (
                    data_source
                    if os.path.basename(data_source).endswith("*.parquet")
                    else os.path.join(data_source, "*.parquet")
                )
                df = cls._check_lazy_load(data_to_read, load_lazy)
            case _:
                raise TypeError(
                    f"Failed to load data_source (type: {type(data_source)}): {data_source}"
                )

    @classmethod
    def _check_lazy_load(
        cls,
        data_to_read: Union[DataSource, str],
        load_lazy: Optional[bool] = None,
        storage_options: Optional[Mapping[str, Any]] = None,
    ) -> Union[pl.DataFrame, pl.LazyFrame]:
        if load_lazy:
            df = pl.scan_parquet(data_to_read, storage_options=storage_options)
        else:
            df = pl.read_parquet(data_to_read, storage_options=storage_options)
        return df

    def get_metadata(self, **kwargs) -> Union[DVBundleMetadata, GenericMetadata]:
        """
        Update the metadata dictionary for a DVBundle. Resets the self.__metadata attribute.
        Use **kwargs to add any additional tags.
        """
        metadata = self.metadata_function(self.data)
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


class DVGrouper(_DataCollection):
    """
    Class for loading and validating groups of closely-related .parquet files into DVBundles (DataFrames with associated metadata).
    Dynamically assigns these data objects as named attributes for access via '.' notation.
    Can produce a unified set of MKDocs-compatible markdown documentation incorporating all data sources included in the collection.
    """

    data: Sequence[Union[DataSource, DVBundle]] = Field(
        ...,
        description="""Collection of DataSources (ParquetFile, DirectoryPath, BlobStorageUrl, or pl.DataFrame) or DVBundles. 
                    Non-parquet files in a directory, as well as any nested subdirectories, will be ignored.""",
        frozen=True,
    )

    require_schema: Optional[bool] = Field(
        False,
        description="""Whether to require specifying a schema when loading in data sources. 
                    If true, requires all data sources in `data` to be provided as DVBundles with associated schema attributes.""",
    )

    markdown_formatter: Callable[
        [Sequence[GenericMetadata], *tuple[Any, ...]], MarkdownOutput
    ] = Field(
        None,  # placeholder - set in class method
        description="An optional function to format metadata into markdown output. Must be consistent with the output of your metadata function.",
        frozen=False,
    )

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
            if self.require_schema and not (isinstance(d, DVBundle) and d.schema):
                error_reasons.add(
                    f"If `self.require_schema`, objects must all be DVBundles with `schema` attribute."
                )
            n_errors += 1
        if error_reasons:
            e = ValueError(f"{n_errors} invalid inputs for `data`: {error_reasons}")
            raise e

        # Load data into bundles and set as attributes
        datasets = []
        for data_source in self.data:
            bundle = (
                data_source
                if isinstance(data_source, DVBundle)
                else DVBundle(
                    data_source=data_source,
                    metadata_function=self.metadata_function,
                    markdown_formatter=self.markdown_formatter,
                )
            )
            bundle.load(
                metadata_on_load=self.metadata_on_load,
                include_timestamp=self.include_timestamp,
                storage_options=self.storage_options,
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