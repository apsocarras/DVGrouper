## Auxiliary/basic types which aren't part of the DVGrouper package data model
import keyword
import glob
from urllib.parse import urlparse
from typing import (
    TypeVar,
    Union,
    Literal,
    ForwardRef,
    Any,
    Optional,
    Mapping,
    Sequence,
    Callable,
    Pattern,
    TypedDict,
    TypeAlias,
    Protocol
)
from pandera import DataFrameModel as pd_DataFrameModel
import polars as pl
from pandera.api.polars.model import DataFrameModel as pl_DataFrameModel
from pydantic import (
    Field,
    model_validator,
    field_validator,
    BaseModel,
    ConfigDict,
    HttpUrl,
    FileUrl,
    validate_call,
    Json,
    FilePath,
    DirectoryPath,
    AnyUrl,
    GetCoreSchemaHandler, 
    TypeAdapter
)
from pydantic_core import CoreSchema, core_schema

from csv import DictReader, DictWriter
import yaml
import os
import json
import re
from typing_extensions import Self
from polars._typing import SizeUnit

from abc import ABC, abstractmethod
from typing import runtime_checkable
import logging


logger = logging.getLogger(__name__)

@runtime_checkable
class NamedDataFrame(Protocol):
    """
    Protocol for a pl.DataFrame with an associated name: ObjectName attribute (needed for inclusion in a DVGrouper)
    """
    name: 'ObjectName'

    def check(obj: object) -> bool:
        return isinstance(obj, NamedDataFrame) and isinstance(obj, pl.DataFrame)

class SizeDesignator(BaseModel):
    """Use to designate size in memory of an entity (kb,b,mb,gb)"""

    size: str = Field(
        ...,
        description="Size of an object in memory (kb,b,mb,gb)",
        pattern=r"[0-9]+ (B|KB|MB|GB)",
    )

    @validate_call
    def create_str(n: int, unit: SizeUnit) -> "SizeDesignator":
        """Create a string which validates against this formatter"""
        return f"{n} {unit.upper()}"

    ## WIP: Figuring out how to make pydantic models into custom types for type annotations (s.t. a str which matches this pattern would pass.)
    # @classmethod
    # def __get_pydantic_core_schema__(
    #     cls
    # ) -> CoreSchema: 
    #     return {"size":self.size}

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
                except TypeError as e:
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
                        except json.JSONDecodeError as e:
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
        return re.sub(cls.regex_separators, "_", s)

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
    def from_data_source(cls, d: 'DataSource') -> 'ObjectName': 
        match d: 
            case pl.DataFrame(): 
                if not NamedDataFrame.check(d): 
                    raise ValueError(f'Cannot get ObjectName from DataFrame without `name` attribute')
                else: 
                    obj_name = ObjectName.parse_object_name(d.name) 
                    return obj_name
            case ParquetFile() | DirectoryPath() | BlobStorageUrl(): 
                basename: str = os.path.basename(d)
                if basename.endswith(".parquet"):
                    basename = basename.rstrip(".parquet")
                obj_name = ObjectName.parse_object_name(basename)
                return obj_name
            case _: 
                raise TypeError(f'Cannot parse object name from {type(d)} (must be valid `DataSource`)')

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
                    


    
YamlFile = JSONRowFile.create(".yaml")
JsonFile = JSONRowFile.create(".json")
CsvFile = JSONRowFile.create(".csv")

DataFrameModel = Union[pl_DataFrameModel]  # TBD on other df libraries
LinkCollection = Sequence[Mapping[str, Union[HttpUrl, FileUrl]]]


DataSource: TypeAlias = Union[ParquetFile, DirectoryPath, BlobStorageUrl, NamedDataFrame]



## TODO: TBD If we support more file formats than .parquet 
# class TabularDataFile(BaseModel):
#     """
#     Model for a source of tabular data (i.e. for a DataFrame).
#     Validates that a file path exists and has the expected file extension.
#     (or is a directory and only contains files with that extension - or SUB-directories with that extension).

#     ## TO-DO: Enable loaders FileUrl
#     """

#     path: Union[FilePath, DirectoryPath, AnyUrl] = Field(
#         ...,
#         description="Path to a file or a directory containing the data source (e.g. a partitioned .parquet).",
#     )
#     file_format: Optional[
#         Union[Literal[".csv", ".parquet"], Sequence[Literal[".csv", ".parquet"]]]
#     ] = Field(
#         None,
#         description="Restrict input file path (or files in directory) to a specified format.",
#     )
#     ignore_regex: Optional[Union[str, Pattern]] = Field(
#         default="\.*.|__init__\.py",
#         description="Regex of files to ignore when checking path (defaults to ignore hidden files and .py initialization files)",
#     )

#     @field_validator("ignore_regex")
#     @classmethod
#     def ensure_valid_regex(cls, v: Any):
#         """
#         Check that the supplied regex to ignore is a valid regex
#         """
#         if isinstance(v, str):
#             re.compile(v)
#         return v

#     @model_validator(mode="before")
#     @classmethod
#     def ensure_path_file_format(cls, data: dict):
#         """
#         Check that path matches the expected file_format
#         """
#         if "file_format" not in data.keys() or not data["file_format"]:
#             return data

#         provided_ext = os.path.splitext(data["path"])[-1]
#         if provided_ext is None:  # i.e. a directory
#             return data

#         elif not (
#             provided_ext == data["file_format"] or provided_ext in data["file_format"]
#         ):
#             raise ValueError(
#                 f"Provided file path {os.path.basename(data['path'])} is not the correct file type (expected: {data['file_format']})"
#             )

#     @model_validator(mode="before")
#     @classmethod
#     def ensure_path_contents_file_format(cls, data: dict):
#         """
#         Check that the directory only contains files of the expected type.
#         """
#         if (
#             "file_format" not in data.keys()
#             or data["file_format"]
#             or not os.path.isdir(data["path"])
#         ):
#             return data

#         unexpected_files = []
#         for root, _, files in os.walk(data["path"]):
#             for file_name in files:
#                 fp = os.path.join(root, file_name)
#                 if not any(file_name.endswith(ext) for ext in data["file_format"]):
#                     if data["ignore_regex"] and not re.search(
#                         data["ignore_regex"], file_name
#                     ):
#                         unexpected_files.append(fp)

#         if unexpected_files:
#             unexpected_files_str = ",".join(
#                 unexpected_files[: min(3, len(unexpected_files))]
#             )
#             if len(unexpected_files) > 3:
#                 unexpected_files_str += ", ..."
#             raise ValueError(
#                 f"{len(unexpected_files)} files found in directory and its subdirectories of unrecognized type ({unexpected_files_str}). Must be {','.join(data['file_format'])}"
#             )

#         return data
