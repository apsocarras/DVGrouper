import pytest
from src.dv_grouper.schemas import SchemaDict
from pandera import DataFrameModel, DataFrameSchema
from pandera.typing import Series
import pandera as pa 

import test.logging_config
import logging 

logger = logging.getLogger(__name__)

class mySimpleDFModel(DataFrameModel): 
    x: Series[str] = pa.Field()
class mySimpleDFModel2(DataFrameModel): 
    z: Series[int] = pa.Field()

def sample_data(): 
    return [
    {'schema_name':'test_name', 'schema':mySimpleDFModel, 'expected_schema_name':'test_name'},
    {'schema_name':None, 'schema':mySimpleDFModel2, 'expected_schema_name':'mySimpleDFModel2'},
    {'schema_name':94, 'expect_schema_name_error':TypeError, 'schema':DataFrameSchema, 'expect_schema_add_error':True, 'expected_schema_name':None}
    ] 

@pytest.fixture
def all_sample_data(): 
    return sample_data()

@pytest.fixture(params=sample_data())
def iter_sample_data(request):
    return request.param 

@pytest.fixture
def default_SchemaDict(all_sample_data): 
    s1,s2 = all_sample_data[0]['schema_name'], all_sample_data[1]['schema_name']
    s = SchemaDict(all_sample_data[1]['schema'], **{s1: all_sample_data[0]['schema']},)    
    return s 

@pytest.fixture
def new_Schema(): 
    class mySimpleDFModel3(DataFrameModel): 
        y: Series[int] = pa.Field()
    return mySimpleDFModel3

def test_SchemaDict_init(iter_sample_data):
    """
    Test initialization and validation with SchemaDict.__init__()
    """
    schema_name = iter_sample_data['schema_name']
    if expected_error := iter_sample_data.get('expect_schema_name_error'): 
        with pytest.raises(expected_error): 
            if schema_name is None: 
                s = SchemaDict(iter_sample_data['schema'])
            else: 
                s = SchemaDict(schema_name=iter_sample_data['schema'])
    else: 
        if schema_name is None: 
            s = SchemaDict(iter_sample_data['schema'])
        else: 
            s = SchemaDict(schema_name=iter_sample_data['schema'])

@pytest.mark.parametrize('exist_mode', ['error', 'warning', 'ignore', 'replace'])
def test_SchemaDict_add(all_sample_data, default_SchemaDict, new_Schema, exist_mode):
  
    logger.info(f'Before add: {default_SchemaDict}')  
    # logger.info(f"{new_Schema}: {type(new_Schema)}")
    # logger.info(new_Schema.__name__)

    logger.info(exist_mode)


    old_schema, old_schema_name = all_sample_data[0]['schema'], all_sample_data[0]['schema_name'], 

    if exist_mode in ('error', 'warning'): 
        raise_type = ValueError if exist_mode == 'error' else Warning
        with pytest.raises(raise_type): 

            ### Attempting to add a schema by a name which is already taken 

            # Add new schema by old name 
            default_SchemaDict.add(schema=new_Schema,
                                    schema_name=old_schema_name, 
                                    exists=exist_mode)

            # Add old schema by old name 
            default_SchemaDict.add(schema=old_schema,
                                schema_name=old_schema_name, 
                                exists=exist_mode)

            # Add old schema directly 
            default_SchemaDict.add(schema=old_schema,
                                exists=exist_mode)
    else: 
        ### Attempting to add a schema by a name which is already taken 


        # Add old schema by old name 
        default_SchemaDict.add(schema=old_schema,
                            schema_name=old_schema_name, 
                            exists=exist_mode)

        # Add old schema directly 
        default_SchemaDict.add(schema=old_schema,
                            exists=exist_mode)
        
        # Add new schema by old name (should replace for newer mode)
        default_SchemaDict.add(schema=new_Schema,
                                schema_name=old_schema_name, 
                                exists=exist_mode)
    
    ### Adding new Schema by a new name 
    default_SchemaDict.add(schema_name=new_Schema.__name__, 
                           schema=new_Schema)
    default_SchemaDict.add(schema=new_Schema, exists='replace')

    # Check if replace worked correctly 
    if exist_mode == 'replace': 

        logger.info(f"old name: {old_schema_name}, new name: {new_Schema.__name__}")
        assert getattr(default_SchemaDict, new_Schema.__name__) == getattr(default_SchemaDict, old_schema_name)

    logger.info(f'After add ({exist_mode}): {default_SchemaDict}')


def test_SchemaDict_remove(all_sample_data, default_SchemaDict, new_Schema):
    logger.info(f'Before remove: {default_SchemaDict}')

    # Attempting to remove a schema which it doesn't have 
    with pytest.raises(AttributeError):
        default_SchemaDict.remove(new_Schema)

    # Removing a schema 
    default_SchemaDict.remove(all_sample_data[0]['schema_name'])
    
    logger.info(f'After remove: {default_SchemaDict}')

