
## For smaller, auxiliary custom types not as integral to the DVGrouper object model. Primarily for hints/validation.  

import keyword
from typing import TypeVar, Union, Literal, ForwardRef, Any, Optional, Mapping, Sequence, Callable, Pattern, TypedDict
from pandera import DataFrameModel as pd_DataFrameModel
from pandera.api.polars.model import DataFrameModel as pl_DataFrameModel
from pydantic import Field, model_validator, field_validator, BaseModel, ConfigDict, HttpUrl, FileUrl, validate_call, Json, FilePath
from csv import DictReader, DictWriter 
import yaml
import os 
import json
import re 
from typing_extensions import Self 

import logging_config 
import logging 

logger = logging.getLogger(__name__)

# 'Pure' Types 
pd_DataFrameType = TypeVar('pandas.core.frame.DataFrame')
pl_DataFrameType = TypeVar('polars.dataframe.frame.DataFrame')
pl_LazyFrameType = TypeVar('polars.lazyframe.frame.LazyFrame')

DataFrameModel = Union[pd_DataFrameModel, pl_DataFrameModel]
DataFrameType = Union[pd_DataFrameType, pl_DataFrameType]

LinkCollection = Union[
    Mapping[str, Union[HttpUrl, FileUrl]],
    Sequence[
        Union[
            Union[HttpUrl, FileUrl],
            Mapping[str, Union[HttpUrl, FileUrl]]
        ]
    ]
]


class SizeString(str):
    """
    String for size in memory.
    """
    n: int
    unit: Literal['mb','kb', 'b']
    def __init__(self, n: int, unit: str):
        if n <= 0:
            raise ValueError("n must be greater than 0")
        if unit.lower() not in ['mb', 'kb', 'b']:
            raise ValueError("unit must be one of 'mb', 'kb', or 'b'")
        
        self.n = n
        self.unit = unit.lower()

    def __str__(self):
        return f"{self.n} {self.unit.upper()}"
    

class TabularDataFile(BaseModel): 
    """
    Model for a source of tabular data (i.e. for a DataFrame).
    Walidates that a file path exists and has the expected file extension.
    (or is a directory and only contains files with that extension - or SUB-directories with that extension).

    ## TO-DO: Enable loaders FileUrl
    """
    path: Union[FilePath, FileUrl] = Field(..., description="Path to a file or a directory containing the data source (e.g. a partitioned .parquet).")
    file_format: Optional[Union[Literal['.csv', '.parquet'], Sequence[Literal['.csv', '.parquet']]]] = Field(None, description="Restrict input file path (or files in directory) to a specified format.")
    ignore_regex: Optional[Union[str, Pattern]] = Field(default='\.*.|__init__\.py', description="Regex of files to ignore when checking path (defaults to ignore hidden files and .py initialization files)") 

    @field_validator('ignore_regex')
    @classmethod
    def ensure_valid_regex(cls, v: Any): 
        """
        Check that the supplied regex to ignore is a valid regex
        """
        if isinstance(v, str):
            re.compile(v)
        return v 

    @model_validator(mode="before")
    @classmethod
    def ensure_path_file_format(cls, data: dict): 
        """
        Check that path matches the expected file_format 
        """        
        if 'file_format' not in data.keys() or not data['file_format']: 
            return data

        provided_ext = os.path.splitext(data['path'])[-1]
        if provided_ext is None: # i.e. a directory  
            return data     
        
        elif not (provided_ext == data['file_format'] or provided_ext in data['file_format']): 
            raise ValueError(f"Provided file path {os.path.basename(data['path'])} is not the correct file type (expected: {data['file_format']})")

    @model_validator(mode="before")
    @classmethod
    def ensure_path_contents_file_format(cls, data: dict): 
        """
        Check that the directory only contains files of the expected type. 
        """
        if 'file_format' not in data.keys() or data['file_format'] or not os.path.isdir(data['path']):
             return data 
    
        unexpected_files = []
        for root, _, files in os.walk(data['path']):
            for file_name in files: 
                fp = os.path.join(root, file_name)
                if not any(file_name.endswith(ext) for ext in data['file_format']): 
                    if data['ignore_regex'] and not re.search(data['ignore_regex'], file_name):
                        unexpected_files.append(fp)

        if unexpected_files:
            unexpected_files_str = ",".join(unexpected_files[:min(3, len(unexpected_files))])
            if len(unexpected_files) > 3:
                unexpected_files_str += ", ..."
            raise ValueError(f"{len(unexpected_files)} files found in directory and its subdirectories of unrecognized type ({unexpected_files_str}). Must be {','.join(data['file_format'])}")

        return data 


class JSONRowFile(BaseModel):
    """
    Class for I/O and validation of files compatible with JSON row operations.
    Create a new for a specific type using the FileType.create() class factory. 
    
    TO-DO: Change so it supports a cloud file source.
    """
    ext: Literal['.yaml', '.json', '.csv'] = Field(..., description='The type of file to validate against')
    path: str
    check_exists: bool = False

    @model_validator(mode='after')
    @classmethod
    def ensure_extension(cls, data: dict): 
        if not os.path.splitext(data['path'])[-1] == data['ext']: 
            raise ValueError(f"{data['path']} does not have a {data['ext']} extension.")
        return data 
    
    @model_validator(mode='after')
    @classmethod
    def ensure_exists(cls, data: dict):
        """
        Check that the provided file exists if check_exists. 
        """
        if data['check_exists'] and not os.path.exists(data['path']): 
            raise FileNotFoundError(f'File {data["path"]} does not exist.')
        return data 

    @classmethod
    def create(cls, ext:str):
        """
        ABC for I/O and validation of a specified type of file. 
        Create a new FileType for a specific type using the FileType.create() class factory 
        and work with the same type of data and methods (JSON/JSON Row data). 

        If extension is YAML or JSON, include load/dump methods.

        YamlFile = FileType.create('.yaml')
        JsonFile = FileType.create('.json')

        """
        class CustomFileType(__class__): 
            
            def __init__(self, path: str, check_exists: bool): 
                super().__init__(path=path, ext=ext, check_exists=check_exists)
                
            match ext: 
                case '.yaml': 
                    def load(self) -> list[dict]: 
                        with open(self.path, 'r') as file: 
                            return [{k:v} for k,v in yaml.safe_load(file)]
                    def dump(self, data: Json) -> list[dict]: 
                        with open(self.path, 'w') as file: 
                            yaml.dump(data, file)                
                case '.json': 
                    def load(self) -> list[dict]: 
                        """
                        Checks if file is a single JSON object or a JSON lines file.  
                        """
                        try: 
                            with open(self.path, 'r') as file: 
                                json_data = json.load(file)
                            return [{k:v} for k,v in json_data]
                        except json.JSONDecodeError as e:
                            result = []
                            with open(self.path, 'r') as file: 
                                for line in file: 
                                    json_object = json.loads(line)
                                    result.append(json_object)
                            return result 
                    def dump(self, data: Json): 
                        with open(self.path, 'w') as file: 
                            json.dump(data, file)

                case '.csv': 
                    def load(self) -> list[dict]: 
                        with open(self.path, 'rb') as file: 
                            csv_reader = DictReader(file)
                        return [row for row in csv_reader] 
                    @validate_call
                    def dump(self, data: Mapping[str, Json]): 
                        """
                        Assumes working with JSON row data.
                        """
                        with open(self.path, 'wb') as file: 
                            csv_writer = DictWriter(file, fieldnames=data[0].keys())
                            csv_writer.writeheader()
                            for row_dict in data: 
                                csv_writer.writer(row_dict)

        CustomFileType.__name__ = f"{ext.lstrip('.').capitalize()}File"
        return CustomFileType
    
    def __str__(self): 
        return self.path
    
YamlFile = JSONRowFile.create('.yaml')
JsonFile = JSONRowFile.create('.json')
CsvFile = JSONRowFile.create('.csv')

class ObjectName(BaseModel):
    """
    Model for a valid object name in Python (excluding reserved keywords).
    """
    name: str = Field(..., description='Name to parse into an object name')
    handler: Literal['parse', 'error'] = Field('error', description='Whether to parse the object name or throw an error')

    model_config = ConfigDict(extra='allow')

    regex_invalid_object_characters = r"^([^a-zA-Z_]+[^a-zA-Z_]*)|[^a-zA-Z0-9_]+"
    regex_reserved_words = r'\b(?:' + '|'.join(re.escape(word) for word in keyword.kwlist) + r')\b'
    regex_separators = "\/|\.|-|,|\\"

    def __init__(self, name: str, handler: Literal['parse', 'error']):
        """
        Data model for a valid python object name. 
        """
    
        self.name = self.parse_object_name(name) if handler == 'parse' else name
        self.handler = handler
        super().__init__(name=self.name, handler=self.handler)

    @classmethod
    def sub_separators(cls, s: str) -> str:
        """
        Replace certain characters ("\/|\.|-|,|\\") with "_"
        """
        return re.sub(cls.regex_separators, '_', s) 
    
    @classmethod
    def remove_invalid(cls, s: str, exclude_reserved=True):
        """
        Remove invalid names for a Python object (with/without excluding reserved keywords).
        """
        regex_to_delete = f'{cls.regex_invalid_object_characters}'
        if exclude_reserved:
            regex_to_delete += "|" + cls.regex_reserved_words
        return re.sub(regex_to_delete, '', s)

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

    @model_validator(mode='before')
    @classmethod
    def ensure_valid_name(cls, d: dict) -> None: 
        """
        Ensure that the given object name is a valid object name or can be parsed to one 
        """
        regex = f'{cls.regex_invalid_object_characters}|{cls.regex_reserved_words}'
        if d['handler'] == 'error': 
            if re.search(regex, d['name']): 
                raise ValueError(f"Supplied str '{d['name']}' is not a valid object name")
        elif d['handler'] == 'parse': 
            cls.parse_object_name(d['name'])
        return d
    

# class PyModule(BaseModel):
#     """
#     Parse a string as to be a Python Module.

#     If check_exists, checks the name as a file path (so not a module listed in the local namespace).
#     """ 
#     name: str 
#     check_exists: bool = False 

#     @field_validator('name')
#     def ensure_name(self, v: Any):
#         for component in re.split('.|\/',v): 
#             if re.search(f"{ObjectName.regex_invalid_object_characters}", component):
#                 raise ValueError(f'Invalid Python Module name ({v})')
#         return v 
    
#     @model_validator(mode='before')
#     @classmethod 
#     def ensure_exists(cls, d: dict): 
#         if d['check_exists'] and not os.path.isfile(d['name']) and not os.path.splitext(d['name']) == '.py':
#             raise ValueError(f'Not an existing Python Module Name ({d['name']})')
#         return d
    
class FunctionMetadata(TypedDict): 
    name: str 
    module: str
    code_source: str     
    doc_string: str 
    func_type: Optional[Literal['creation', 'transformation', 'reference']] = None

    @classmethod
    def extract(cls, func: Callable, func_type: Optional[Literal['creation', 'transformation', 'reference']] = None)  -> 'FunctionMetadata': 
        """
        Extract a function's metadata. Provide the type of function it is (rel. to a give DataFrame).
        """
        
        return cls(name=func.__name__, 
                   module=func.__module__, 
                   code_source=f"{func.__code__.co_filename}:{func.__code__.co_firstlineno}", 
                   doc_string=func.__doc__ or '',
                   func_type=func_type 
                   )

class FunctionRegistry(BaseModel): 
    """
    Container to register functions associated with a given DataFrame (DVBundle())
    """
    funcs: Sequence[Callable] # will be cast to list 
    metadata: Sequence[FunctionMetadata]

    def register_func(self, func: Callable, func_type: Optional[Literal['creation', 'transformation', 'reference']]) -> None: 
        """
        Add a function to the registry. 
        """
        self.funcs = sorted(list(self.funcs) + [func])  
        func_metadata = FunctionMetadata.extract(func, func_type=func_type)
        self.metadata = func_metadata
    
    def remove_func(self, func: Optional[Callable] = None, func_name: Optional[str] = None) -> None:
        """
        Remove a function from the registry (either give func itself or its name)
        """ 
        assert any(x for x in (func, func_name))
        
        if func: 
            self.funcs = [x for x in self.funcs if x != func]
            self.metadata.pop(func.__name__)
        elif func_name:
            self.metadata.pop(func_name)
            self.funcs = [x for x in self.funcs if x.__name__ != func_name] 
            
        
class DescLink(TypedDict): 
    name: Optional[str] = None
    description: str 
    links: str 

class DataFrameDescription(DescLink): 
    columns: DescLink 

class DescriptionFile(JSONRowFile):
    """
    File (YAML, JSON, CSV) containing manual descriptions of DataFrames and columns.
    Intended for use with multiple DataFrames (i.e. a DVBundleStore() object). 
    
    All of the below formats will be parsed to JSON row style list:
        [{"name":..., "description":..., "links":..., "columns":{...}}]
    
    YAML/JSON:
    <DataFrameName>:
        description: 
        links:
        columns:
            <ColumnName>:
                description: 
        ...

    CSV: 
    name, description, links, columns 

    JSON-ROW: 
    [{"name":..., "description":..., "links":..., "columns":{...}}]
    """
    ext: Literal['.yaml', '.json', '.csv']
    check_exists: bool = True
    
    @property
    def content(self) -> list[dict[str, Json]]: 
        return self.load()
    

    @model_validator(mode='after')
    def ensure_expected_fields(self) -> Self: 
        """
        (TO-DO): Ensure only (and all of) the required columns are included in the Description file for all rows (leave values blank if you want to omit).
        """




