import os 

import pandas as pd 

import polars as pl

from re import sub
from .funcs.utils import get_df_metadata_dict, combine_dicts, get_consecutive_year_ranges, us_state_map
from config import get_logger, set_logging_level
import logging
from typing import Literal, Union, List, Optional
from collections.abc import Sequence 
from .schemas import DataPath


CUR_DIR = os.path.dirname(os.path.abspath(__file__))  
PAR_DIR = os.path.dirname(CUR_DIR)
DATA_DIR = os.path.join(PAR_DIR, 'data')

logger = get_logger(__name__)
set_logging_level(logging.DEBUG)

class DVGrouper():
    def __init__(self,
                 engine: Literal['pandas', 'polars', 'pyspark'], 
                 paths: Sequence[str],  
                 expected_files: Optional[Sequence[str]] = None, 
                 file_formats: Union[Literal[".parquet", ".csv"], Sequence[Literal['.csv', '.parquet']]] = ".parquet"):
        """
        Initialize class for loading and validating groups of related DataFrames. 

        Args: 

            engine (Literal['pandas', 'polars', 'pyspark']): Which DataFrame library you wish to use. 
            paths (Sequence[str]): Set of (absolute) file paths or directories containing DataFrames matching allowed file formats. 
            expected_files (set): Optional set of filenames to expect when reading in data to use as a filter. 
            file_formats (Union[Literal[".parquet", ".csv"], List[Literal['.csv', '.parquet']]])): Accepted file formats to load (can be a single format or a list of formats). 
        """ 
        # Filter provided paths vs expected files prior to loading
        self._filter_expected_vs_given_paths(expected_files, paths)

        # Load/validate files 
        self._data_dict = {}
        self.metadata = {}
        DVGrouper.load_data_dict(self._paths, self.file_formats, self._data_dict, self.metadata, include_metadata=True)

        # Unpack contents as attributes
        self._set_attributes_from_data_dict()
        self._data_dict = None
        self.datasets = tuple(sorted(self.metadata.keys()))
   
    ## Instance Methods ##

    def add_df(self, df:Optional[Literal[pd.DataFrame, pl.DataFrame]]): 
        """
        (TO-DO): 
        """

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
    
    def _filter_expected_vs_given_paths(self, expected_files:set, paths:set) -> None: 
        """
        Filter the list of passed file names vs the expected file names.
        Sets   
        """

        self._expected_files = set([DVGrouper._standard_basename(f) for f in expected_files]) if expected_files else set({})
        given_files = set([DVGrouper._standard_basename(f) for f in paths])

        missing_files = self._expected_files.difference(given_files)
        if len(missing_files) > 0:
            error_msg = f'{len(missing_files)} file names missing in provided paths vs. expected:\n({given_files})\n({missing_files})' 
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        extra_files = given_files.difference(self._expected_files)
        if len(extra_files) > 0: 
            logger.warning(f'{len(extra_files)} additional files included in provided paths vs. expected (see self._unexpected_files). Filtering before loading.')
            self._paths = tuple(sorted((p for p in paths if os.path.splitext(os.path.basename(p))[0] in given_files)))
            self._unexpected_files = extra_files
        else: 
            self._paths = tuple(sorted(paths))

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
            DataPath(path=path, file_format=file_format)

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
    def _parse_attr_name(cls, file_name:str) -> str: 
        """
        Parse the name of a file containing a DataFrame so that it can be set as an instance attribute. 
        """
        known_prefixes = ("exp","data", "Data", 'table')
        s = sub('|'.join(known_prefixes), '', file_name).lstrip('.')
        s = sub('\.', '_', s)
        return s
    
    @classmethod
    def _parse_expected_files(cls, file_names:tuple) -> set: 
        """
        Takes a tuple of file names and returns a set parsed with _parse_attr_name. 
        """
        return {DVGrouper._standard_basename(f) for f in file_names}
    
    @classmethod
    def _get_path_to_data(cls, da_type:str) -> tuple: 
        """
        Return the path to the default data directory associated with the given DVGrouper and all contained files.
        
        Args: 

        da_type (str): The type of DVGrouper

        abs_path (bool): Whether to give absolute paths in list of data files. 

        Returns: 

        dir_name (str): Name of directory containing data
        
        abs_paths (set): Set of file names contained in dir_name

        """
        assert da_type in ("benefits", "expenses", "geog", "jobs", "taxes")

        dir_name = os.path.join(DATA_DIR, 'parquet', da_type)
        abs_paths = {os.path.join(dir_name, f) for f in os.listdir(dir_name)}

        return dir_name, abs_paths

    @classmethod
    def _standard_basename(cls, file_name, file_format='parquet') -> str: 
        """
        Return a path basename without its file extension. 
        """

        return sub(f'\.{file_format}','', os.path.basename(file_name))

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

