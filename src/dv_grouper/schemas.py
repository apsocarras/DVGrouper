from pydantic import BaseModel,  FilePath, DirectoryPath, ValidationError, model_validator, Field, field_validator
import os 
from typing import Literal, Union, List, Any, Optional
from re import compile, search


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
            
        

    
        

