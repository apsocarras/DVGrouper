from pydantic import BaseModel,  FilePath, DirectoryPath, ValidationError, model_validator, Field, field_validator, ConfigDict, validate_call
import os 
from typing import Literal, Union, List, Any, Optional, Dict, Sequence, Mapping
from re import compile, search
from pandera import DataFrameModel, DataFrameSchema
import re 
from copy import copy

class DataPath(BaseModel): 
    """
    Class for validating that a file path exists and has the expected file extension 
    (or is a directory and only contains files with that extension - or SUB-directories with that extension).
    """
    path: Union[FilePath, DirectoryPath] = Field(..., description="Path to a file or a directory.")
    file_format: Optional[Union[Literal['.csv', '.parquet'], List[Literal['.csv', '.parquet']]]] = Field(None, description="Restrict input file path (or files in directory) to specified format.")
    ignore_regex: Optional[str] = Field(default='\.*.|__init__\.py', description="Regex of files to ignore when checking dir_path (defaults to ignore hidden files and .py initialization files)")

    @field_validator('ignore_regex')
    @classmethod
    def ensure_valid_regex(cls, v: Any): 
        """
        Check that the supplied regex to ignore is a valid regex
        """
        try: 
            compile(v)
        except Exception as e:
            raise ValueError(f'Failed to compile regex supplied for ignore_regex ({v}): ({e})') 

    @model_validator(mode="before")
    @classmethod
    def ensure_path_file_format(cls, data: dict): 
        """
        Check that path matches the expected file_format 
        """        
        print(data, type(data))
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
                    if data['ignore_regex'] and not search(data['ignore_regex'], file_name):
                        unexpected_files.append(fp)

        if unexpected_files:
            unexpected_files_str = ",".join(unexpected_files[:min(3, len(unexpected_files))])
            if len(unexpected_files) > 3:
                unexpected_files_str += ", ..."
            raise ValueError(f"{len(unexpected_files)} files found in directory and its subdirectories of unrecognized type ({unexpected_files_str}). Must be {','.join(data['file_format'])}")

        return data 
            
class AttributeName(BaseModel):
    """
    Data model for a valid class attribute name. 
    """
    name: str 
    model_config = ConfigDict(extra='allow')

    @field_validator('name')
    @classmethod
    def ensure_name(cls, v: Any) -> None: 
        """
        Ensure that the given attribute name is a validate attribute name.
        """
        regex = r'^([^a-zA-Z_]+[^a-zA-Z_]*)|[^a-zA-Z0-9_]+'
        if re.search(regex, v): 
            raise ValueError(f"Supplied str '{v}' is not a valid attribute name")
        return v

    @classmethod
    def parse_attribute_name(cls, v: str) -> str: 
        """ 
        Removes and replaces values in a str to become a valid attribute name. Returns ValueError if the resulting string is empty.
        """
        to_replace = r'\/|\.|-|,'
        replaced = re.sub(to_replace, '_', v)
        to_delete = r'^([^a-zA-Z_]+[^a-zA-Z_]*)|[^a-zA-Z0-9_]+'
        deleted = re.sub(to_delete, '', replaced)

        if len(deleted) == 0: 
            raise ValueError(f'Unable to parse str "{v}" to a valid attribute name.')

        return deleted

    

class SchemaDict(BaseModel, validate_assignment=True): 
    """
    Data model for accepting a list of Pandera DataFrameModels or arbitrary key-value pairs of Pandera DataFrameModels.
    Re-validates when the model is changed. 
    """
    model_config = ConfigDict(extra='allow')
    
    @validate_call
    def __init__(self, *args:Union[DataFrameModel,  Sequence[DataFrameModel]], **kwargs: DataFrameModel):
        """
        Args: 
        (*args): One or more DataFrame models. Will be added to SchemaDict via their same names 
        (**kwargs): Key-value pairs of str-DataFrameModel. Use if you want to rename the model while assigning it to the SchemaDict
        """
        super().__init__()
        for a in args: 
            setattr(self, a.__name__, a)
        for k,v in kwargs.items(): 
            setattr(self, k, v)

    def add(self, schema:DataFrameModel, schema_name: Optional[str] = None, exists: Literal['error', 'warning', 'ignore', 'replace'] = 'error') -> None:
        """
        Add a DataFrameModel to the SchemaDict.
        
        Args: 
        schema: DataFrameModel to validate an incoming DataFrame against. 
        schema_name: Name to rename the incoming schema when setting as an attribute. 
        exists: If a schema of the same name already exists in the list, set whether to raise an error, warning, ignore, or replace with the new schema.
        """

        schema_name_attr = schema_name if schema_name else schema.__name__ 
    
        try: 
            if attr_set := getattr(self, schema_name_attr): 
                message = f"Schema of the same name already present ({schema_name_attr}={attr_set})"
                match exists: 
                    case 'error': 
                        raise ValueError(message)
                    case 'warning': 
                        raise Warning(message)
                    case 'ignore': 
                        pass 
                    case 'replace': 
                        setattr(self, schema_name_attr, schema)            
        except AttributeError:
            setattr(self, schema_name_attr, schema)
            
    def remove(self, schema: Union[str, DataFrameModel], /):
        """
        Remove a schema from the SchemaDict.  
        """        
        schema_name = schema if isinstance(schema, str) else schema.__name__
        delattr(self, schema_name)

    def __repr__(self): 
        cls = self.__class__.__name__
        return f"{cls}({', '.join(tuple(f'{k}={v}' for k,v in self.dict().items()))})"
    def __str__(self):
        cls = self.__class__.__name__
        return f"{cls}({', '.join(tuple(f'{k}={v}' for k,v in self.dict().items()))})"

