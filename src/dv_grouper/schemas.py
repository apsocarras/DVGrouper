from pydantic import BaseModel,  FilePath, DirectoryPath, model_validator, Field, field_validator, ConfigDict, validate_call, FileUrl
from csv import DictWriter, DictReader

import pandas as pd 
import polars as pl  
import yaml
import json
from typing import Callable
import os 
from typing import Literal, Union, List, Any, Optional, Dict, Sequence, Mapping, TypedDict
from re import compile, search
from dv_grouper.types import *
import datetime as dt

from typing import TypeVar, ForwardRef
from functools import wraps

import re 

from .config import Config
import logging_config
import logging 

from .funcs.utils import named_class_factory
from .funcs.decorators import register_function

logger = logging.getLogger(__name__)


## -- Pydantic Models -- ##

class PanderaSchemaDict(BaseModel, validate_assignment=True): 
    """
    Data model for accepting a list of Pandera DataFrameModels or arbitrary key-value pairs of Pandera DataFrameModels.
    Re-validates when the model is changed. 
    """
    model_config = ConfigDict(extra='allow')
    
    @validate_call
    def __init__(self, *args:Union[DataFrameModel,  Sequence[DataFrameModel]], **kwargs: DataFrameModel):
        """
        Args: 
        (*args): One or more DataFrame models. Will be added to PanderaSchemaDict via their same names 
        (**kwargs): Key-value pairs of str-DataFrameModel. Use if you want to rename the model while assigning it to the PanderaSchemaDict
        """
        super().__init__()
        for a in args: 
            setattr(self, a.__name__, a)
        for k,v in kwargs.items(): 
            parsed_attr_name = ObjectName(name=k, handler='error')
            setattr(self, parsed_attr_name, v)
            

    def add_schema(self, schema:DataFrameModel, schema_name: Optional[str] = None, exists: Literal['error', 'warning', 'ignore', 'replace'] = 'error') -> None:
        """
        Add a DataFrameModel to the PanderaSchemaDict.
        
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
            
    def remove_schema(self, schema: Union[str, DataFrameModel], /):
        """
        Remove a schema from the PanderaSchemaDict.  
        """        
        schema_name = schema if isinstance(schema, str) else schema.__name__
        delattr(self, schema_name)

    def __repr__(self): 
        cls = self.__class__.__name__
        return f"{cls}({', '.join(tuple(f'{k}={v}' for k,v in self.dict().items()))})"
    def __str__(self):
        cls = self.__class__.__name__
        return f"{cls}({', '.join(tuple(f'{k}={v}' for k,v in self.dict().items()))})"

class ColumnMetadata(BaseModel): 
    """
    Data Model for the metadata on the columns of a DataFrame Column.
    """
    name: str 
    dtype: Any
    unique_vals: Sequence[Any] = Field(..., description='Unique values in the column (may be up to a maximum)')
    n_total: int
    n_unique: int
    n_null: int
    size: int = None  
    description: Optional[str] = None

class DataFrameMetadata(BaseModel):
    """
    Data Model for the metadata on the entire DataFrame. 
    """
    size: int
    shape: tuple[int, int]
    n_null: int 
    n_duplicates: int
    
    column_metadata: Mapping[str, ColumnMetadata]

    # Manual/External Metadata
    data_source: Optional[TabularDataFile] = Field(None, description='File path (or URL) to the DataFrame source data')
    links: Optional[LinkCollection] = Field(None, description='Any other weblinks (e.g. GitHub, cloud storage, background information, etc.)')
    description: Optional[str] = Field(None, description='Any other notes or description on the data source')
    functions: Optional[FunctionRegistry] = None

    @classmethod
    def create(cls, name: str):
        return named_class_factory(cls, name)

    def to_meta_file(self, path: str, file_overwrite=False, chunk_overwrite=False, include_timestamp=True) -> None:
        """
        Write DataFrameMetadata to an existing YAML or JSON file.
        """
        if file_ext := os.path.splitext(path)[-1] not in ('.yaml', '.json'): 
            raise ValueError(f"{path} must have extension '.yaml' or '.json'")
        
        metadata_file = YamlFile(path=path, check_exists=False) if file_ext == '.yaml'\
            else JsonFile(path=path, check_exists=False)
        
        new_metadata_chunk = {self.__name__:self.metadata}
        if include_timestamp:  
            cur_time = dt.datetime.now().strftime('%d-%m-%Y_%H:%M:%S')
            new_metadata_chunk['time_written'] = cur_time

        if not os.path.exists(metadata_file.path): 
            metadata_file.dump(new_metadata_chunk)
        elif not file_overwrite:
            raise FileExistsError(f'Metadata file {path} exists but file_overwrite={file_overwrite}') 
        else: 
            extant_data = metadata_file.load(path)
            old_chunk = None # for outputting in log message
            if self.__name__ in extant_data:
                if not chunk_overwrite: 
                    raise ValueError(f"Metadata for DVBundle() {self.__name__} is already in metadata file, but chunk_overwrite={chunk_overwrite}")
                else: 
                    old_chunk = extant_data.pop(self.__name__)
            extant_data[self.__name__] = new_metadata_chunk

            if Config.LOGLEVEL == logging.DEBUG:

                def dict_diff(a,b):
                    return  {k:v for k,v in a.items() if a.get(k) != b.get(k)}
                # Only want to check these if we're debugging
                old_dict_diff = dict_diff(old_chunk, new_metadata_chunk)
                new_dict_diff = dict_diff(new_metadata_chunk, old_chunk)

                logger.debug(f"Replaced metadata block in {metadata_file.path} (Diff Old: {old_dict_diff} ) (Diff New: {new_dict_diff})")


class DVBundle(BaseModel): 
    """
    Class for associating a DataFrame with its validation schema, metadata, description, and functions.
    Think of a DVBundle as everything associated with a given data source and how you are using it in your program. 
    
    TO-DO: Checking typing references to DataFrameMetadata

    Register the function which created the DataFrame for this DVBundle using a decorator.
        Saves the path to the .py file in the current directory which houses the function, and a str representation of the function formatted in a markdown block. 
    Register a transformation function to this DataFrame using a decorator. 
        Register another DVBundle as the target of this transformation function using the same decorator. 
    Add YAML block of metadata to an existing YAML file. 
    Generate automated markdown documentation block for this bundle (its metadata, its transformation function). 
    """
    model_config = ConfigDict(extra='allow')

    df: DataFrameType = Field(..., description=f"The DataFrame associated with this DVBundle()")
    path: TabularDataFile
    validation_schema: Optional[DataFrameModel]

    description: Optional[str] = Field(None, description='Description of the DataFrame. If the DVBundle() is included in a DVBundleStore(), this will be overwritten by its description in the group\'s DescriptionFile (if it has one).')
    data_source: Optional[TabularDataFile] = None
    links: Optional[LinkCollection] = None 
    column_metadata: Optional[Sequence[ColumnMetadata]] = Field(None, description='Metadata on the columns. If the DVBundle() is included in a DVBundleStore(), this will be overwritten by any column metadata listed in the group\'s DescriptionFile (if any).')
    functions: FunctionRegistry = None

    def __init__(self, **kwargs): 
        super().__init__(**kwargs)
        self.update_metadata(self.df)


    @classmethod
    def create(cls, name: Union[str, ObjectName], **kwargs) -> ForwardRef('DVBundle'):
        """
        Class factory for creating custom named DVBundles(). Useful to associate it with the name of its DataFrame.
        """ 
        return named_class_factory(cls, name)


    def update_metadata(self, 
                    size_unit: Literal['mb','kb','b'] = 'mb',
                    regen_df: bool = False,
                    description: Optional[str] = None,
                    column_metadata: Optional[Sequence[ColumnMetadata]] = None,
                    links: Optional[LinkCollection] = None,
                    data_source: Optional[TabularDataFile] = None, 
                    functions: Optional[FunctionRegistry] = None, 
                    ) -> DataFrameMetadata: 
        """
        Update the metadata dictionary for a DVBundle. Resets the self.__metadata attribute. 
        metadata = {
                    self.__name__: {
                        'size': f"{<int>} {size_unit}",
                        'shape': tuple[int, int],
                        'description': str,
                        'links': LinkCollection,
                        'functions':dict[FunctionMetadata]
                        'columns': {
                            <col_name>:{
                                'dtype': <dtype>,
                                'n_total': <int>, 
                                'n_unique': <int>,
                                'n_null': <int>,
                                'unique_vals': <list>,
                                'size': f"{<int>} {size_unit}"
                                'description':<str>
                            }
                            ...
                        }                         
                }

        Args: 
            size_unit (str): Unit to display sizes of DF and columns. 
            regen_df (bool): Whether to re-scan the DF to re-produce the DF metadata. 
        """
        try: 
            current_metadata_dict = self.__metadata 
        except AttributeError:
            current_metadata_dict = {} 

        if regen_df:
            # Get metadata from dataframe  
            match self.df: 
                case pl.DataFrame | pl.LazyFrame:
                    df_metadata = current_metadata_dict if not regen_df else \
                        {self.__name__:self._get_polars_metadata(self.df,
                                                self.functions,  
                                                size_unit=size_unit)}
                # case pd.DataFrame:
                # case pyspark.sql.DataFrame

            df_metadata['functions'] = self.functions
            df_metadata['data_source'] = self.data_source
            df_metadata['links'] = self.links
            df_metadata['description'] = self.description
            
        else: 
            df_metadata = current_metadata_dict
            for attr_name, new_param_val in zip(('functions', 'data_source', 'links', 'description', 'column_metadata'), (functions, data_source, links, description, column_metadata)): 
                # Updating instance attributes
                if new_param_val: 
                    logger.info(f'Updating {attr_name} attribute (old: {getattr(self, attr_name)}, new: {new_param_val})')
                    setattr(self, attr_name, new_param_val)
                # Updating metadata dict 
                if df_metadata.get(attr_name) != getattr(self, attr_name): 
                    logger.info(f'Updating metadata dict for {attr_name} (old: {df_metadata.get(attr_name)}, new: {getattr(self, attr_name)})')
                    new_value = getattr(self, attr_name)
                    df_metadata[attr_name] = new_value
        
        # Update metadata dict 
        self.__metadata = df_metadata
        return df_metadata
    

    @property
    def metadata(self): 
        # Update the metadata (without reloading the dataframe) silently 
        self.update_metadata(regen_df=False,
                             description=self.description, 
                             column_metadata=self.column_metadata, 
                             links=self.links, 
                             data_source=self.data_source, 
                             functions=self.functions, 
                             )
        return self.__metadata 
 
    @classmethod
    def _get_polars_metadata(cls, 
                             df: Union[pl.DataFrame, pl.LazyFrame], 
                             n_unique_include: int = 10, 
                             size_unit: Literal['mb', 'kb', 'b'] = 'b') -> DataFrameMetadata: 
        """
        Get metadata on the columns of a polars DataFrame
        """

        ## TO-DO: Which is faster? This or the DataFrame concatenation?
        # df_counts = df.select(
        #     *(pl.col(col).alias(f'{col}_n_unique').approx_n_unique() for col in df.columns),
        #     *(pl.col(col).len().alias(f'{col}_n_total') for col in df.columns)
        #     *(pl.col(col).null_count().alias(f'{col}_n_total') for col in df.columns)
        # ).to_dict(as_series=False)

        df_count_unique = df.select(pl.all().approx_n_unique())\
            .rename({col:f"{col}_n_unique" for col in df.columns})
        df_count_total = df.select(pl.all().len())\
            .rename({col:f"{col}_n_total" for col in df.columns})
        df_count_null = df.select(pl.all().null_count())\
            .rename({col:f"{col}_n_null" for col in df.columns})
        df_counts_dict = pl.concat([df_count_unique,df_count_total, df_count_null], how='horizontal').to_dict(as_series=False)
        
        col_types = dict(df.schema)
            
        column_metadata = {
            col:ColumnMetadata(**{   
                    # 'name': col,
                    'dtype':col_types[col],

                    'n_total':df_counts_dict[f'{col}_n_total'][0],
                    'n_unique':df_counts_dict[f'{col}_n_unique'][0],
                    'n_null':df_counts_dict[f'{col}_n_null'][0],

                    'unique_vals': df[col].unique().head(n_unique_include).to_list(),
                    'size':str(SizeString(n=df.select(col).estimated_size(size_unit.lower()), unit=size_unit)),
                    'description':None

                })
            for col in df.columns
            }
        
        df_metadata = DataFrameMetadata({
            'size': f"{df.estimated_size(unit=size_unit)} {size_unit}",
            'shape': df.shape,
            'n_null': sum((c['n_null'] for c in column_metadata)),
            'n_duplicates': df.is_duplicated().len(),
            'columns': column_metadata,
            'functions':None, # These will be added from the instance attributes. 
            'links': None, # These will be added from the description file. 
            'description': None, # These will be added from the description file. 
        })

        return df_metadata

    def register_func(self, func: Callable, func_type: Optional[Literal['creation', 'transformation', 'reference']]):
        """
        (TO-DO): Add a function to the registry. 
        Wrapped by funcs.decorators.register_func().
        Wrapper for types.FunctionRegistry.register_func(). 
        """
        func


    def remove_func(self, func: Optional[Callable] = None, func_name: Optional[str] = None) -> None:
        """
        Remove a function from the registry (either give func itself or its name)
        """ 


class DVBundleStore(): 
    """
    Class to store DVBundles

    Generate automated YAML file from all DataFrames in the bundle.
    Read a description yaml file which contains manual descriptions of the DataFrames and its columns.  
    """

    @validate_call
    def add_dataframe(self, 
            df:Optional[DataFrameModel] = None, 
            from_file: Optional[Union[TabularDataFile, str]] = None, 
            schema: Optional[Union[PanderaSchemaDict, str]] = None, 
            overwrite: bool = False,
            as_copy: bool = True):
        """
        Add a DataFrame to the DVBundleStore, either from an existing DataFrame or from a file. 

        Args: 
            df: Either a Polars DataFrame/LazyFrame, Pandas DataFrame, or PySpark DataFrame. Type must match self.engine.
            from_file: Path to a .parquet or .csv file. 
            schema: DataFrameModel to validate against incoming df (You must explicitly add it to self.schemas first!).
            overwrite: Whether to replace a DataFrame of the same name if it exists already.  
            copy: Whether to add the DataFrame as a copy or reference.
        """ 

    
