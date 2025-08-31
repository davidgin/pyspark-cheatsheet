"""
Type checking utilities for PySpark project to improve type safety.
"""
from typing import Any, Dict, List, Union, Type, TypeVar, get_type_hints, get_origin, get_args
from pydantic import BaseModel, ValidationError
import inspect

T = TypeVar('T')

class TypeSafetyChecker:
    """Utility class for runtime type checking and validation"""
    
    @staticmethod
    def validate_type_annotation(obj: Any, expected_type: Type) -> bool:
        """
        Check if object matches the expected type annotation at runtime
        
        Args:
            obj: The object to check
            expected_type: The expected type (can include Union, List, Dict, etc.)
            
        Returns:
            bool: True if the object matches the expected type
        """
        try:
            # Handle Union types (e.g., Union[str, int])
            origin = get_origin(expected_type)
            
            if origin is Union:
                # Check if obj matches any of the union types
                union_args = get_args(expected_type)
                return any(isinstance(obj, arg) for arg in union_args if arg != type(None))
                
            elif origin is list:
                # Check List[T] types
                if not isinstance(obj, list):
                    return False
                list_type = get_args(expected_type)[0] if get_args(expected_type) else Any
                if list_type == Any:
                    return True
                return all(isinstance(item, list_type) for item in obj)
                
            elif origin is dict:
                # Check Dict[K, V] types
                if not isinstance(obj, dict):
                    return False
                dict_args = get_args(expected_type)
                if len(dict_args) != 2:
                    return True  # Dict without type args
                key_type, value_type = dict_args
                return (all(isinstance(k, key_type) for k in obj.keys()) and
                       all(isinstance(v, value_type) for v in obj.values()))
                
            else:
                # Simple type check
                return isinstance(obj, expected_type)
                
        except Exception:
            return False
    
    @staticmethod
    def check_function_types(func: callable) -> callable:
        """
        Decorator that validates function arguments and return types at runtime
        
        Args:
            func: Function to validate
            
        Returns:
            Decorated function with type checking
        """
        type_hints = get_type_hints(func)
        signature = inspect.signature(func)
        
        def wrapper(*args, **kwargs):
            # Bind arguments to parameters
            bound_args = signature.bind(*args, **kwargs)
            bound_args.apply_defaults()
            
            # Check argument types
            for param_name, param_value in bound_args.arguments.items():
                if param_name in type_hints:
                    expected_type = type_hints[param_name]
                    if not TypeSafetyChecker.validate_type_annotation(param_value, expected_type):
                        raise TypeError(
                            f"Argument '{param_name}' expected type {expected_type}, "
                            f"got {type(param_value)} with value: {param_value}"
                        )
            
            # Call the function
            result = func(*args, **kwargs)
            
            # Check return type
            if 'return' in type_hints:
                return_type = type_hints['return']
                if not TypeSafetyChecker.validate_type_annotation(result, return_type):
                    raise TypeError(
                        f"Return value expected type {return_type}, "
                        f"got {type(result)} with value: {result}"
                    )
            
            return result
        
        return wrapper
    
    @staticmethod
    def find_any_types_in_module(module_name: str) -> List[str]:
        """
        Find all usage of Any type in a module
        
        Args:
            module_name: Name of the module to check
            
        Returns:
            List of locations where Any is used
        """
        try:
            import importlib
            module = importlib.import_module(module_name)
            any_usage = []
            
            for name, obj in inspect.getmembers(module):
                if inspect.isfunction(obj) or inspect.ismethod(obj):
                    try:
                        type_hints = get_type_hints(obj)
                        for param, hint in type_hints.items():
                            if hint == Any or 'Any' in str(hint):
                                any_usage.append(f"{name}.{param}: {hint}")
                    except Exception:
                        continue
                        
            return any_usage
        except Exception as e:
            return [f"Error analyzing module {module_name}: {e}"]

class PydanticTypeChecker:
    """Enhanced type checking using Pydantic for data validation"""
    
    @staticmethod
    def create_strict_validator(data_type: Type[BaseModel]):
        """
        Create a strict validator function for a Pydantic model
        
        Args:
            data_type: Pydantic model class
            
        Returns:
            Validator function that raises detailed errors
        """
        def validate(data: Dict[str, Any]) -> data_type:
            try:
                return data_type(**data)
            except ValidationError as e:
                # Provide detailed error information
                error_details = []
                for error in e.errors():
                    field = ".".join(str(loc) for loc in error['loc'])
                    error_details.append(f"Field '{field}': {error['msg']}")
                
                raise ValueError(f"Validation failed for {data_type.__name__}:\n" + 
                               "\n".join(error_details))
        
        return validate
    
    @staticmethod
    def batch_validate_with_details(data_list: List[Dict[str, Any]], 
                                   model_class: Type[T]) -> Dict[str, Union[List[T], List[str]]]:
        """
        Validate a batch of data with detailed error reporting
        
        Args:
            data_list: List of dictionaries to validate
            model_class: Pydantic model class to validate against
            
        Returns:
            Dictionary with 'valid' and 'errors' keys
        """
        valid_records = []
        error_records = []
        
        validator = PydanticTypeChecker.create_strict_validator(model_class)
        
        for i, data in enumerate(data_list):
            try:
                validated_record = validator(data)
                valid_records.append(validated_record)
            except ValueError as e:
                error_records.append(f"Record {i}: {str(e)}")
        
        return {
            'valid': valid_records,
            'errors': error_records,
            'valid_count': len(valid_records),
            'error_count': len(error_records),
            'success_rate': len(valid_records) / len(data_list) if data_list else 0.0
        }

def check_for_any_usage() -> None:
    """Utility function to check for Any type usage in project files"""
    project_modules = [
        'models',
        'schemas',
        'shuffle_optimization',
        'partitioning_strategies',
        'main',
        'pydantic_examples'
    ]
    
    print("[ANALYZING] Checking for Any type usage in project modules:")
    print("=" * 60)
    
    for module in project_modules:
        try:
            any_usage = TypeSafetyChecker.find_any_types_in_module(module)
            if any_usage:
                print(f"\n[MODULE] Module: {module}")
                for usage in any_usage:
                    print(f"   [WARNING] {usage}")
            else:
                print(f"[SUCCESS] Module: {module} - No Any types found")
        except Exception as e:
            print(f"[ERROR] Module: {module} - Error: {e}")
    
    print("\n" + "=" * 60)
    print("[TIP] Consider replacing Any types with more specific Union types")
    print("[TIP] Use TypeVar for generic functions")
    print("[TIP] Use Pydantic models for structured data validation")

if __name__ == "__main__":
    check_for_any_usage()