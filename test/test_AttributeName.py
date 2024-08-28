import pytest 
from src.dv_grouper.schemas import AttributeName
from pydantic import ValidationError
import test.logging_config
import logging


@pytest.fixture(params=[
        {'input_name':'%,sfd', 'expected_input_error':ValueError, 'expected_parsed_name':'_sfd', 'expected_parsed_error':None},
        {'input_name':'$1,s', 'expected_input_error':ValueError, 'expected_parsed_name':'_s', 'expected_parsed_error':None},
        {'input_name':',4', 'expected_input_error':ValueError, 'expected_parsed_name':'_4', 'expected_parsed_error':None},
        {'input_name':'12^', 'expected_input_error':ValueError, 'expected_parsed_name':None, 'expected_parsed_error':ValueError},
        {'input_name':'abc', 'expected_input_error':None, 'expected_parsed_name':'abc', 'expected_parsed_error':None},
        {'input_name':'example/file/path.json', 'expected_input_error':ValueError, 'expected_parsed_name':'example_file_path_json', 'expected_parsed_error':None},
    ])
def sample_data(request): 
        return request.param
                        
def test_AttributeName_init(sample_data):
    """
    Test the AttributeName class for validating str as attribute names. 
    """
    if sample_data['expected_input_error']: 
        with pytest.raises(ValidationError): 
            attr_name = AttributeName(name=sample_data['input_name'])
    else: 
        attr_name = AttributeName(name=sample_data['input_name'])

def test_AttributeName_parse_attribute_name(sample_data): 
    """
    Test the AttributeName.parse_attribute_name method for parsing str into valid attribute names. 
    """
    if sample_data['expected_parsed_error']: 
        with pytest.raises(ValueError): # Need to give the right sort of error to expect 
            parsed_attr_name = AttributeName.parse_attribute_name(sample_data['input_name'])
            assert parsed_attr_name == sample_data['expected_parsed_name']
    else:         
        parsed_attr_name = AttributeName.parse_attribute_name(sample_data['input_name'])
        assert parsed_attr_name == sample_data['expected_parsed_name']
