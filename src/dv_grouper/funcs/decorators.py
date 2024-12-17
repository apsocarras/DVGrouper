from typing import Callable, Type, TypedDict, Any, Literal
from typing import Callable, Type, TypedDict, Literal, Any
from ..types import FunctionMetadata, DescriptionFile

# Check if it has the attribute
def check_registry(cls, attr_name, create_registry=False):
    try:          
        getattr(cls, attr_name)
    except AttributeError as e: 
        if create_registry: 
            setattr(cls, attr_name, {})
        raise AttributeError(f"Target class {cls} does not have {attr_name} registry") 

def register_function(target_class: Type[Any], 
                      function_type: Literal['creation', 'reference'], 
                      create_registry: bool = False):

    def decorator(func: Callable[..., Any]):
        metadata: FunctionMetadata = {
            'name': func.__name__,
            'module': func.__module__,
            'code_source': func.__code__.co_filename,
            'description': func.__doc__ or ""
        }

        check_registry(target_class, 'functions', create_registry)

        target_class.functions[func.__name__] = {
            'type': function_type,
            **metadata
        }
    return decorator