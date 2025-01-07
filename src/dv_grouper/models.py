"""
Module containing auxiliary models other than the two main models of the dv_grouper package (DVGrouper, DVBundle)
"""

import json
import keyword
import logging
import os
import re
import warnings
from abc import ABC, abstractmethod
from collections.abc import Collection
from csv import DictReader, DictWriter
from datetime import datetime
from types import FunctionType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Union,
    runtime_checkable,
)
from urllib.parse import urlparse

import pandera as pa
import polars as pl
import yaml
from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    DirectoryPath,
    Field,
    FilePath,
    FileUrl,
    Json,
    model_validator,
    validate_call,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dv_grouper._types import DataSource, GenericMetadata


@runtime_checkable
class NamedDataFrame(Protocol):
    """
    Protocol for a pl.DataFrame with an associated name: ObjectName attribute (needed for inclusion in a DVGrouper)
    (Not a pydantic BaseModel since it's not meant to be instantiated.)
    """

    name: "ObjectName"

    def check(obj: object) -> bool:
        return (
            isinstance(obj, NamedDataFrame)
            and isinstance(obj, pl.DataFrame)
            and isinstance(obj.name, "ObjectName")
        )


class ColumnMetadata(BaseModel):
    """
    Data Model for the metadata on the columns of a DataFrame Column.
    """

    model_config = ConfigDict(
        frozen=True, arbitrary_types_allowed=True
    )  # Deliberately create a new object rather than changing anything here

    name: str
    dtype: pl.DataType
    size: str  # "SizeDesignator"
    example_values: tuple[Any]  # pl.DataType
    n_rows: int
    n_unique: int
    n_null: int
    description: Optional[str] = None
    range_values: Optional[Sequence[Any]] = Field(  # pl.DataType
        default=None, description="Range of values for e.g. a date column"
    )


class DVBundleExternalMetadata(BaseModel):
    """
    Manually-added/"external" metadata for a DataFrame (i.e. not tied strictly to the DataFrame at a given time)
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: "ObjectName" = Field(
        ...,
        description="Descriptive name for the DataFrame associated with this DVBundle.",
        frozen=True,
    )
    orig_source: Optional[Union[FilePath, DirectoryPath]] = Field(
        None,
        description="File path (or URL) to the DataFrame source data; this might be omitted if the DVBundle loaded a DataFrame directly",
        frozen=True,
    )
    schema: pa.DataFrameModel = Field(
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
            setattr(func, "func_type", func_type)
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
    def to_markdown(cls, metadata: "DVBundleMetadata") -> "MarkdownOutput":
        """
        TODO: create a block of markdown text.
        """
        pass


class JSONRowFile(ABC):
    """
    ABC for I/O and validation of files compatible with JSON row operations (JSON, YAML, CSV).
    Create a new subclass for a specific file type using the FileType.create_type() class factory.
    """

    ext: Literal[".yaml", ".json", ".csv"]
    path: str
    check_exists: bool

    @abstractmethod
    def load(self) -> "JSONRowFile":
        pass

    @abstractmethod
    def dump(self) -> None:
        pass

    @classmethod
    def create(
        cls,
        ext: str,
        doc_string: Optional[str] = None,
    ) -> "JSONRowFile":
        """
        Create a new subclass for a specific file type.
        Used for creating metadata output files (JSON, YAML, CSV).
        """

        class CustomFileType(cls):
            def __init__(self, path: str, check_exists: bool):
                # validate inputs
                if check_exists and not os.path.exists(path):
                    raise FileNotFoundError(f"File {path} does not exist.")
                try:
                    if os.path.splitext(path)[-1] != ext:
                        raise ValueError(
                            f"Provided path {path} does not match extension: {ext}"
                        )
                except TypeError:
                    raise ValueError(f"Provided path {path} must be valid  ")

                # initialize
                self.path = path
                self.check_exists = check_exists

                super().__init__(path=path, ext=ext, check_exists=check_exists)

            match ext:
                case ".yaml":

                    def load(self) -> list[dict]:
                        with open(self.path, "r") as file:
                            return [{k: v} for k, v in yaml.safe_load(file)]

                    def dump(self, data: Json) -> list[dict]:
                        with open(self.path, "w") as file:
                            yaml.dump(data, file)
                case ".json":

                    def load(self) -> list[dict]:
                        """
                        Checks if file is a single JSON object or a JSON lines file.
                        """
                        try:
                            with open(self.path, "r") as file:
                                json_data = json.load(file)
                            return [{k: v} for k, v in json_data]
                        except json.JSONDecodeError:
                            result = []
                            with open(self.path, "r") as file:
                                for line in file:
                                    json_object = json.loads(line)
                                    result.append(json_object)
                            return result

                    def dump(self, data: Json):
                        with open(self.path, "w") as file:
                            json.dump(data, file)
                case ".csv":

                    def load(self) -> list[dict]:
                        with open(self.path, "rb") as file:
                            csv_reader = DictReader(file)
                        return [row for row in csv_reader]

                    @validate_call
                    def dump(self, data: Mapping[str, Json]):
                        """
                        Assumes working with JSON row data.
                        """
                        with open(self.path, "wb") as file:
                            csv_writer = DictWriter(file, fieldnames=data[0].keys())
                            csv_writer.writeheader()
                            for row_dict in data:
                                csv_writer.writer(row_dict)

        CustomFileType.__name__ = f"{ext.lstrip('.').capitalize()}File"
        CustomFileType.__doc__ = doc_string
        return CustomFileType

    def __str__(self):
        return self.path


YamlFile = JSONRowFile.create(".yaml")
JsonFile = JSONRowFile.create(".json")
CsvFile = JSONRowFile.create(".csv")


class ObjectName(BaseModel):
    """
    Model for a valid object name in Python (excluding reserved keywords).
    """

    name: str = Field(..., description="Name to parse into an object name")
    handler: Literal["parse", "error"] = Field(
        "error", description="Whether to parse the object name or throw an error"
    )

    model_config = ConfigDict(extra="allow")

    _regex_invalid_object_characters = r"^([^a-zA-Z_]+[^a-zA-Z_]*)|[^a-zA-Z0-9_]+"
    _regex_reserved_words = (
        r"\b(?:" + "|".join(re.escape(word) for word in keyword.kwlist) + r")\b"
    )
    _regex_separators = "\/|\.|-|,|\\"

    def __init__(self, name: str, handler: Literal["parse", "error"]):
        """
        Data model for a valid python object name.
        """

        self.name = self.parse_object_name(name) if handler == "parse" else name
        self.handler = handler
        super().__init__(name=self.name, handler=self.handler)

    @classmethod
    def sub_separators(cls, s: str) -> str:
        """
        Replace certain characters ("\/|\.|-|,|\\") with "_"
        """
        return re.sub(cls._regex_separators, "_", s)

    @classmethod
    def remove_invalid(cls, s: str, exclude_reserved=True):
        """
        Remove invalid names for a Python object (with/without excluding reserved keywords).
        """
        regex_to_delete = f"{cls._regex_invalid_object_characters}"
        if exclude_reserved:
            regex_to_delete += "|" + cls._regex_reserved_words
        return re.sub(regex_to_delete, "", s)

    @classmethod
    def parse_object_name(cls, s: str) -> str:
        """
        Removes and replaces values in a str to become a valid object name. Returns ValueError if the resulting string is empty.

        Replaces separators (/\.-,) with _
        Removes other invalid characters.
        """
        s = cls.sub_separators(s)
        s = cls.remove_invalid(s)
        if len(s) == 0:
            raise ValueError(f'Unable to parse str "{s}" to a valid object name.')
        return s

    @classmethod
    def from_data_source(cls, d: DataSource) -> "ObjectName":
        match d:
            case ParquetFile() | DirectoryPath() | BlobStorageUrl():
                basename: str = os.path.basename(d)
                if basename.endswith(".parquet"):
                    basename = basename.rstrip(".parquet")
                obj_name = ObjectName.parse_object_name(basename)
                return obj_name
            case _:
                raise TypeError(
                    f"Cannot parse object name from {type(d)} (must be one of: ParquetFile, DirectoryPath, BlobStorageUrl)"
                )

    @model_validator(mode="before")
    @classmethod
    def ensure_valid_name(cls, d: dict) -> None:
        """
        Ensure that the given object name is a valid object name or can be parsed to one
        """
        regex = f"{cls._regex_invalid_object_characters}|{cls.regex_reserved_words}"
        if d["handler"] == "error":
            if re.search(regex, d["name"]):
                raise ValueError(
                    f"Supplied str '{d['name']}' is not a valid object name"
                )
        elif d["handler"] == "parse":
            cls.parse_object_name(d["name"])
        return d

    @classmethod
    def from_df(
        cls, df: Union[pl.DataFrame, pl.LazyFrame], use_parsed_name: bool
    ) -> "ObjectName":
        if not NamedDataFrame.check(df):
            raise ValueError(
                "Cannot get ObjectName from DataFrame without `name` attribute"
            )
        else:
            obj_name = ObjectName(
                name=df.name, handler="parse" if use_parsed_name else "error"
            )
            return obj_name


class SizeDesignator(BaseModel):
    """Use to designate size in memory of an entity (kb,b,mb,gb)"""

    n: float = Field(...)
    size_unit: str = Field(
        ...,
        description="Size of an object in memory (kb,b,mb,gb)",
        pattern=re.compile(r"[0-9]+ (B|KB|MB|GB)", flags=re.IGNORECASE),
    )

    def __str__(self) -> str:
        """Create a string which validates against this formatter"""
        return f"{self.n} {self.size_unit.upper()}"


class BlobStorageUrl(AnyUrl):
    """
    Model to check whether a link to a .parquet file (or directory) is housed in a supported cloud storage platform.
    You can define credentials for specific cloud providers at a DVGrouper level or individual DVBundle level.

    NOTE: If a url fails to validate against this model, the DVBundle will attempt a basic read of the url with pl.read_parquet(); this will require the blob to be publicly accessible.
    """

    allowed_schemes = {"https"}

    url: FileUrl = Field(
        ...,
        description="Path to a .parquet file hosted in blob storage (Azure, AWS, GCP supported). Must be publicly available or you must set authorization information in the DVBundle/DVGrouper.",
    )

    @model_validator(mode="before")
    @classmethod
    def validate(cls, val: Union[dict, str]) -> Union["BlobStorageUrl", bool]:
        """Check for valid Azure, S3, or GCP storage url"""
        match val:
            case dict():
                url = val.get("url", None)
                if not url:
                    raise ValueError('Missing "url" attribute')

                cloud_provider = cls.get_cloud_provider(url)
                if cloud_provider not in {"Azure", "AWS", "GCP"}:
                    raise ValueError(
                        "The URL must point to an Azure Blob Storage, Amazon S3, or GCP Storage endpoint."
                    )
            case str():
                return cls.get_cloud_provider(url) in {"Azure", "AWS", "GCP"}
        return

    @classmethod
    def get_cloud_provider(
        cls, url: str
    ) -> Union[Literal["Azure", "AWS", "GCP"], None]:
        """
        Check whether the url is from a supported cloud provider.
        """
        parsed_url = urlparse(url)
        hostname = parsed_url.hostname
        if hostname:
            if hostname.endswith(".blob.core.windows.net"):
                return "Azure"
            elif hostname.endswith(".s3.amazonaws.com"):
                return "AWS"
            elif hostname.endswith(".storage.googleapis.com"):
                return "GCP"
        return None


class MarkdownOutput(BaseModel):
    """
    TODO: Validate text string for valid markdown syntax.
    """

    pass


class ParquetFile(BaseModel):
    file_path: FilePath

    @model_validator(mode="before")
    @classmethod
    def validate_ext(cls, val: Union[dict, str]):
        match val:
            case dict():
                fp = val.get("file_path", "")
                if not os.path.splitext(fp)[1] == ".parquet":
                    raise ValueError(f"{fp} is not a valid .parquet file")
                return val
            case str():
                return os.path.splitext(val)[1] == ".parquet"

    @model_validator(mode="before")
    @classmethod
    def check_exists(cls, val: Union[dict, str]):
        match val:
            case dict():
                fp = val.get("file_path", "")
                if not os.path.isfile(fp):
                    raise ValueError(f"{fp} does not exist.")
                return val
            case str():
                return os.path.isfile(fp)


class LoadOptions(BaseModel):
    """
    Model for the data loading options you can set when defining a DVBundle or DVGrouper.
    """

    storage_options: Optional[Mapping[str, Any]] = Field(
        None,
        description="""A dictionary of credentials (e.g., API keys) required for data access. Defaults to None. See polars documentation for valid options.
        Caution: You may want to use this on load() rather than persist them along with the object.
        """,
    )
    metadata_on_load: Optional[bool] = Field(
        False,
        description="Whether to load metadata on along with data (default is False)",
        frozen=False,
    )
    load_lazy: Optional[bool] = Field(
        True, description="Whether to load in DataFrames lazily or eagerly."
    )

    require_schema: Optional[bool] = Field(
        False,
        description="""Whether to require specifying a schema when loading in data sources or adding existing DataFrames/LazyFrames.""",
    )
    reload_dataframe: Optional[bool] = Field(
        False,
        description="""Whether to re-load a DataFrame/LazyFrame from source if .load() is called and .df is already set.""",
    )

    add_timestamp: Optional[bool] = Field(
        False,
        description="Whether to include a timestamp in the metadata upon loading.",
    )


class MetadataOptions(BaseModel):
    """
    Model for the metadata options you can set when defining a DVBundle or DVGrouper.
    """

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

    _time_loaded: Optional[datetime] = (
        None  # Internal field to mark when data was last read
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)
