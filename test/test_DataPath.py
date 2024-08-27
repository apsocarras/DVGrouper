import pytest 
import pandas as pd 
import numpy as np 
import os 
from typing import Literal
from src.dv_grouper.schemas import DataPath
import test.logging_config
import logging

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def tmp_file(tmp_path_factory, request) -> None:
    """
    Create temporary DataFrame data files for the session.
    """ 

    file_name = os.path.splitext(os.path.basename(request.param))[0]

    # Seed seed for consistency 
    np.random.default_rng(12345)
    df_dummy = pd.DataFrame(np.random.randint(0,100, size=(100, 4)), columns=['years', 'fruit', '9', 'random_name'])
    
    parquet_path = tmp_path_factory.mktemp('data') / f"{file_name}.parquet"
    csv_path = tmp_path_factory.mktemp('data') / f"{file_name}.csv"

    df_dummy.to_parquet(parquet_path)
    df_dummy.to_csv(csv_path)
    
    return {'parquet_path':str(parquet_path), 'csv_path': str(csv_path)}

@pytest.mark.parametrize('tmp_file', ['dummy_data'], indirect=True)
def test_DataPath(tmp_file):
    parsed_parquet = DataPath(path=tmp_file['parquet_path'])
    parsed_csv = DataPath(path=tmp_file['csv_path'])

    logger.info(parsed_parquet)
    logger.info(parsed_csv)
 


