from pydantic import BaseModel,  FilePath, DirectoryPath, ValidationError, model_validator, Field, field_validator
import os 
from typing import Literal, Union, List, Any, Optional
from re import compile, search


class DataPath(BaseModel): 
    """
    Class for validating that a file path exists and has the expected file extension 
    (or is a directory and only contains files with that extension - or SUB-directories with that extension).
    """
    path: Union[FilePath, DirectoryPath]
    file_format: Union[Literal['.csv', '.parquet'], List[Literal['.csv', '.parquet']]]
    ignore_regex: Optional[str] = Field(default='\.*.|__init__\.py', description="Regex of files to ignore when checking dir_path (defaults to ignore hidden files and .py initialization files)")

    @model_validator
    @classmethod
    def ensure_match_expected(cls, data: dict): 
        """
        Check that file_path matches the expected file_format 
        """        
        provided_ext = os.path.splitext(data['path'])
        if provided_ext is None: 
            pass # i.e. a directory - need to check contents 
        elif not (provided_ext == data['file_format'] or provided_ext in data['file_format']): 
            raise ValueError(f"Provided file path {os.path.basename(data['path'])} is not the correct file type (expected: {data['file_format']})")

    @model_validator
    @classmethod
    def ensure_only_expected_file_types(cls, data: dict): 
        """
        Check that the directory only contains files of the expected type. 
        """
        if not os.path.isdir(data['path']): 
            return 
        
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
            raise ValueError(f"{len(unexpected_files)} files found in directory and its subdirectories of unrecognized type ({unexpected_files_str}). Must be {",".join(data['file_format'])}")

            
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

        

