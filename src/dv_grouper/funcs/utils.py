## Small wrapper functions

import pandas as pd 
import polars as pl 
import bisect 
from typing import Union
### ------------------------------------------------------------------------------ ###
### --- GENERAL UTILS  --- ###

def get_closest_n(n, ls:list, sort=True): 
    """Match n to closest element in (sorted) ls. n and ls must be numeric."""
    if sort: 
        ls.sort()
    idx = bisect.bisect_left(ls, n) # where you'd insert n s.t. e in a[:i] -> e < n and e in a[i:] -> e >= n
    if idx == 0: # n is same as first element  
        closest = ls[0]
    elif idx == len(ls): # n is same as last element
        closest = ls[-1]
    else:  
        before_element = ls[idx-1] 
        after_element = ls[idx]
        closest = before_element if after_element-n > n-before_element else after_element
    return closest

def get_closest_year(year_col:list, years_available:list, sort=True) -> pd.Series:
    """Creates a year-key column for reconciling discrepancies in years between datasets"""
    assert pd.api.types.is_list_like(year_col)
    assert pd.api.types.is_list_like(years_available)

    if sort: 
        years_available = sorted(years_available)

    return pd.Series([get_closest_n(year, years_available) for year in year_col])

def get_consecutive_year_ranges(years:list):
    if not years:
        return ["NA"]
    elif not pd.api.types.is_list_like(years): 
        raise TypeError(f"'years' must be a list or 'list-like'")
        
    years.sort()  # Ensure the years are sorted
    ranges = []
    start = years[0]
    end = years[0]

    for year in years[1:]:
        if int(year) == int(end) + 1:
            end = year
        else:
            if start == end:
                ranges.append(f"{int(start)}")
            else:
                ranges.append(f"{int(start)}-{int(end)}")
            start = year
            end = year

    # Append the final range
    if start == end:
        ranges.append(f"{int(start)}")
    else:
        ranges.append(f"{int(start)}-{int(end)}")

    return ranges

def get_df_metadata_dict(df:Union[pd.DataFrame, pl.DataFrame], df_name=None, include_size='MB', size_mode='total', **kwargs) -> dict: 
    """
    Create dict containing metadata of a Pandas or Polars DataFrame. 

    **(kwargs): Include arbitrary key-value pairs in resulting metadata dict. 
    """

    if include_size is not None: 
        assert include_size in ('MB', 'KB', 'B')
    
    if size_mode is not None: 
        assert size_mode in ('total', 'cols')

    # Years
    years_of_data = []
    for year_col in ('ruleYear', 'yearofdata', 'Year', 'LatestYear', 'year'): 
        if year_col in df.columns: 
            years_of_data = df[year_col].dropna().unique().tolist()
            years_of_data.sort()

    # Column Dtypes and sizes  
    col_types = df.infer_objects().dtypes.apply(lambda x: str(x)).to_dict()
    if include_size: 
        col_sizes = df.memory_usage()
        total_size = col_sizes.sum() # bytes 
        size_map = {'MB':1000000,'KB':1000, "B":1}
        total_size /=  size_map[include_size] 
        col_sizes = (col_sizes / size_map[include_size]).to_dict()

    # Join dicts: 
    schema_dict = {}
    for col in df.columns: 
        schema_dict[col] = {'type':col_types[col]}
        if include_size and size_mode == 'cols': 
            schema_dict[col][f'size_{include_size}'] = col_sizes[col]
        
    # Index Col
    try: 
        index_col = list(df.columns).index('Unnamed: 0')
        schema_dict.pop('Unnamed: 0')
    except ValueError as e: 
        index_col = None # All should have an index column  

    info_dict = {'name':df_name} if df_name else {} # ensure df_ma,e it's at the "front" of the dict, if it's available 

    info_dict['years_of_data'] = get_consecutive_year_ranges(years_of_data)
    info_dict[f'total_size_{include_size}'] = total_size
    info_dict['index_col'] = index_col
    info_dict['schema'] = schema_dict

    for k,v in kwargs.items(): 
        info_dict[k] = v

    return info_dict
    
def combine_dicts(*dicts):
    combined_dict = {}
    for d in dicts:
        if d == {}:
            continue
        else:
            try:
                combined_dict.update(d)
            except Exception as e: 
                raise Exception(f"Failed to combine {d} in {dicts} ({str(e)})")
    return combined_dict