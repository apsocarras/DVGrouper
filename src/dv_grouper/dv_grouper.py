
# DataFrame Libraries: 
import pandas as pd 
import polars as pl
# import pyspark.sql.

import os 
from re import sub, search
from .funcs.utils import get_df_metadata_dict, combine_dicts, get_consecutive_year_ranges

from functools import wraps

# Typing 
from typing import Literal, Union, Optional, Mapping
from collections.abc import Sequence 
from .schemas import TabularDataFile, SchemaDict, AttributeName, DataFrameModel, DataFrame, RegexPattern
from pydantic import validate_call, Field, BaseModel, FilePath, DirectoryPath, model_validator
from re import Pattern

import logging

CUR_DIR = os.path.dirname(os.path.abspath(__file__))  
PAR_DIR = os.path.dirname(CUR_DIR)

logger = logging.getLogger(__name__)

class DVGrouper(BaseModel):
    """
    Model for loading and validating groups of related DataFrames. Lazy loads for polars and pyspark.
    """ 

    engine: Literal['pandas', 'polars', 'pyspark'] = Field(..., description='Which DataFrame library you wish to use. Polars and PySpark require installing optional dependencies.')
    paths: Optional[Sequence[Union[FilePath, DirectoryPath]]] = Field(None, description="Set of (absolute) file paths or directories containing DataFrames matching allowed file formats.")
    expected_files: Optional[Sequence[Union[FilePath, DirectoryPath], str]] = Field(None, description="Set of (absolute or basename) file paths to expect when reading in paths. Can use as a filter in the case of reading from a directory.")
    regex_ignore: Optional[Union[str, RegexPattern]] = Field(None, description="Regex pattern of files to ignore from paths (e.g. \.gitkeep).")
    if_extra_files: Literal['warning', 'error', 'ignore'] = Field('warning', description='Set behavior for when unexpected filenames are passed in to be read.')
    file_format: Union[Literal[".parquet", ".csv"], Sequence[Literal['.csv', '.parquet']]] = Field()
    
    schemas: Optional[
        Union[
            Mapping[str, DataFrameModel], 
            Sequence[DataFrameModel], 
            DataFrameModel, 
            SchemaDict
            ]] = Field(None, description="Pandera DataModels to check against before reading in a DataFrame. Use key-value pairs to rename incoming Schemas.")
    dataframes: Optional[Union[Sequence[DataFrame], DataFrame]] = Field(None, description="DataFrames to add when initializing the DVGrouper(). Can only provide one of dataframes or paths.")
    
    require_schema: bool = Field(..., "Whether to require specifying a schema in self.schemas before reading in DataFrames")

    def __init__(self,**data):
        
        super().__init__(**data)
        
        # Set Schemas 
        if self.schemas and not isinstance(self.schemas,SchemaDict): 
            self.schemas = SchemaDict(self.schemas) 
        
        # Filter provided paths prior to loading
        self._filter_given_paths(self.expected_files, self.paths, self.regex_ignore, self.if_extra_files)


        # Load/validate files 
        self._data_dict = {}
        self.metadata = {}
        DVGrouper.load_data_dict(self._paths, self.file_formats, self._data_dict, self.metadata, include_metadata=True)

        # Unpack contents as attributes
        self._set_attributes_from_data_dict()
        self._data_dict = None
        self.datasets = tuple(sorted(self.metadata.keys()))

    ## Validators ##
    @model_validator(mode="before")
    @classmethod
    def ensure_expected_files(cls, data: dict): 
        """
        Ensure the provided files match the expected files. 
        """
        if paths := data.get('paths'):
            cls._filter_given_paths(given_files=paths, 
                                    expected_files=data['expected_files'], 
                                    regex_ignore=data['regex_ignore'], 
                                    if_extra_files=data['if_extra_files'])
            
        return data 

    @model_validator(mode="before")
    @classmethod
    def ensure_paths_or_dfs(cls, data: dict): 
        """
        Ensure exactly one of paths or dataframes is provided 
        """
        if data.get('paths') and data.get('dataframes'): 
            raise ValueError(f'Provide exactly one of "paths" or "dataframes"')
        return data 
            
    ## Instance Methods ##
    @wraps(SchemaDict.add)
    def add_schema(self, schema:DataFrameModel, schema_name: Optional[str] = None, exists: Literal['error', 'warning', 'ignore', 'replace'] = 'error') -> None:
        """
        Add a named Pandera schema to the DVGrouper() to validate against an incoming DataFrame.
        """ 
        self.schemas.add(schema, schema_name, exists)

    @wraps(SchemaDict.remove)
    def remove_schema(self, schema: Union[str, DataFrameModel], /):
        """
        Remove a schema from self.schemas 
        """
        self.schemas.remove(schema)
        
    @validate_call
    def add_df(self, 
               df:Optional[DataFrameModel] = None,
               from_file: Optional[TabularDataFile] = None,
               schema: Optional[Union[SchemaDict, str]] = None,
               overwrite: bool = False,
               ) -> None: 
        """
        (TO-DO): Add a DataFrame to the DVGrouper, either from an existing DataFrame or from a file. 
        
        Args: 
        df: Either a Pandas, Polars, or PySpark DataFrame. Type must match self.engine.
        from_file: Path to a .parquet or .csv file. 
        schema: DataFrameModel to validate against incoming df (You must explicitly add it to self.schemas first!).
        overwrite: Whether to replace a DataFrame of the same name if it exists already.  
        """
        if (not any(df, from_file)) or all(df, from_file):
            raise ValueError("Provide exactly one of df: DataFrame or file_path to a DataFrame")

        elif from_file: 
            
            fp = TabularDataFile(path=from_file)
            file_ext = fp.file_format

            # Parse the DataFrame name to be set as an attribute  
            df_name = AttributeName.parse_attribute_name(str(fp.path))
            
            # Check if there's a DataFrame of the same name   
            try: 
                if isinstance(has_df := getattr(self, df_name), DataFrame): 
                    if overwrite: 
                        logger.warning(f'Overwriting set DataFrame {df_name}')
                        pass 
                    else: 
                        raise ValueError(f'DataFrame{df_name} already set.')
            except: 
                pass # TO-DO

            
            match (self.engine, file_ext): 
                # Polars load the LazyFrame    
                case ('polars', '.parquet'): 
                    df = pl.scan_parquet(fp)
                case ('polars', '.csv'): 
                    df = pl.scan_csv(fp)

                ## TO-DO: Other engines 
                # case ('pandas', '.parquet'):

                # case ('pandas', '.csv'): 

                # case ('pyspark', '.parquet'):
                # case ('pyspark', '.csv') 

            # Obtain metadata from the DataFrame (TO-DO: Write class method with different engines)

            # 

                

            
        

    def _set_attributes_from_data_dict(self) -> None: 
        """ 
        Set the DataFrame contents of the data_dict from load_data_dict as separate instance attributes. 

        Currently load_data_dict() is a class method which doesn't require an instance of the class and modifies a dict in place recursively.

        Calling _set_attributes_from_data_dict() requires copying the contents of this dict, which is time consuming. 

        TO-DO: May want to consolidate load_data_dict() and _set_attributes_from_data_dict() to be an instance method which can directly set the DataFrames as attributes. 
        """
        
        for dir_name, df_dict in self._data_dict.items(): 
            for df_name, df_content in df_dict.items(): 
                # Parse DataFrame name so it's a valid attribute name 
                df_name_parsed = DVGrouper._parse_attr_name(df_name)
                # Set the attribute
                setattr(self, df_name_parsed, df_content['data'])
    
    @classmethod
    def _filter_given_paths(cls, 
                            given_files:Sequence[Union[str, FilePath, DirectoryPath]], 
                            expected_files: Optional[Sequence[Union[str, FilePath, DirectoryPath]]], 
                            regex_ignore: Optional[Union[str, RegexPattern]] = None,
                            if_extra_files: Literal['warning', 'error', 'ignore'] = 'warning',
                            ) -> tuple[tuple, tuple]: 
        """
        Filter passed file paths vs expected file paths.

        Returns: 
        paths (tuple): Paths filtered by expected_files and regex_ignore. 
        extra_files (tuple): Extra files not in expected_files.
        """
        if not expected_files and not regex_ignore: 
            raise ValueError("Must provide at least one of 'expected_files' or 'regex_ignore' to filter given_files")
        
        # Check vs expected files 
        if expected_files:
            given_files_parsed = set(cls._standard_basename(f) for f in given_files)
            expected_files_parsed = set(cls._standard_basename(f) for f in expected_files) 

            missing_files = expected_files_parsed.difference(given_files_parsed)
            if len(missing_files) > 0:
                error_msg = f'{len(missing_files)} file names missing in provided paths vs. expected:\n({given_files})\n({missing_files})' 
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            extra_files = given_files_parsed.difference(expected_files_parsed)
            
            if len(extra_files) > 0: 
                msg = f'{len(extra_files)} additional files included in provided paths vs. expected.'
                match if_extra_files: 
                    case 'ignore': 
                        pass 
                    case 'warning': 
                        msg += ' Filtering before loading.'
                        logger.warning(msg)
                    case 'error': 
                        raise ValueError(msg)
                    
            # Check vs regex 
            if regex_ignore:
                paths = tuple(sorted((p for p in given_files if cls._standard_basename(p) in expected_files_parsed and search(p, regex_ignore))))
            else: 
                paths = tuple(sorted((p for p in given_files if cls._standard_basename(p) in expected_files_parsed)))
        
        # Check vs regex 
        elif regex_ignore:
            paths = tuple(sorted((p for p in given_files if search(p, regex_ignore))))

        return paths, extra_files

    ## Class Methods ##
            
    def load_data_dict(self, engine:Literal['pandas', 'polars'], paths:Sequence[str], file_format:Sequence[str], data_dict:dict, metadata_dict:dict, include_metadata=True) -> None: 
        
        """
        Load DataFrames into grouped dict; modifies provided data_dict and metadata_dict in place to enable recursive calling. 
            Switched to only support .parquet files for speed and simplification of code (previously was working to also support .pkl and .csv)
        
        Args: 

        data_dict (dict): Dict with DataFrames and optional metadata.

            TO-DO: Include schema of data dict here 

        metadata_dict (dict): Separate dict to store just the metadata     

        include_metadata (bool): Whether to update the dict containing metadata on DataFrames    

        """

        for path in paths: 
            
            # Validate path 
            TabularDataFile(path=path, file_format=file_format)

            # If a file, add to the data dictionary 
                
            try: # path is a file  
                DVGrouper._validate_path(path, file_format, load_dirs=False)

                dir_name = os.path.basename(os.path.dirname(path)) # name to add to data_dict
                ff = os.path.splitext(path)[-1]
                path_basename = os.path.basename(os.path.splitext(path)[0]) # exclude format from name 
                path_basename = DVGrouper._parse_attr_name(path_basename) # parse to match attribute names 
                full_path = path # keep raw path for metadata

                # Read data 
                data = pd.read_parquet(path)

                # Get metadata 
                metadata = get_df_metadata_dict(data, **{'path':full_path, 'format':ff}) if include_metadata else {}
                
                # Add to top level metadata dict
                if include_metadata: 
                    metadata_dict[path_basename] = {k:v for k,v in metadata.items()}

                # Check metadata for index col and set 
                if metadata and metadata['index_col'] is not None: 
                    data.set_index(data.columns[0], inplace=True)
                    data.index.name = 'index'

                # Add to top level data_dict
                if dir_name not in data_dict.keys():
                    data_dict[dir_name] = {}

                data_dict[dir_name][path_basename] = combine_dicts({'data':data}, metadata)
    
            except ValueError as e: # path is a directory  

                # validate the path as directory  
                DVGrouper._validate_path(path, file_formats, load_dirs=True)

                for fp in os.listdir(path): 
                    # construct full path and cast to list
                    full_path = [os.path.join(path, fp)]
                    # call load_data_dict() recursively on full path 
                    cls.load_data_dict(full_path, file_formats, data_dict, metadata_dict, include_metadata)

            except TypeError as e: # file or directory failed validation
                pass # TO-DO

        return
    

    @classmethod
    def _standard_basename(cls, file_name, file_format='parquet') -> str: 
        """
        Return a path basename without its file extension. 
        """

        return sub(f'\.{file_format}','', os.path.basename(file_name))

    ## Properties   
    @property
    def schemas(self):
        return self.__schemas
   

    ## Overriding Default Methods 
    def __repr__(self): 
        """Print representation of the DVGrouper. Includes name of data set, years available, and (TO-DO: Data Set Description?)"""
        # Calculate the maximum length of the DataFrame names
        max_name_length = max(len(df_name) for df_name in self.metadata.keys())
        s = f"{'Dataset':<{max_name_length}} {'Years Available'}\n"
        s += f"{'--------':<{max_name_length}} {'-----------------'}\n"
        for df_name, m in sorted(self.metadata.items(), key=lambda x: x[0]):
            years = m['years_of_data']
            s += f"{df_name:<{max_name_length}} ({', '.join(years)})\n"
            
        return s    


    def search_cols(self, 
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
            df: pd.DataFrame = getattr(self, df_name) 
            matches = set()
            lookup_strs = []
            for c in cols: 
                if isinstance(c, Pattern) or use_regex: 
                    regex_matches = set(df.filter(regex=c).columns)
                    matches.update(regex_matches)
                elif isinstance(c, str): 
                    lookup_strs.append(c)
                else: 
                    raise TypeError(f'{c} must be `Pattern` or `str` (given: {type(c)})')
            if len(lookup_strs) > 0: 
                exact_matches = set(df.filter(lookup_strs).columns)
                matches.update(exact_matches)
            if len(matches) > 0: 
                result[df_name] = list(matches)
        return result 
