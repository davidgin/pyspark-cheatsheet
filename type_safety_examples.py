"""
Examples demonstrating improved type safety in the PySpark project.
"""
from typing import Union, List, Dict
from models import (
    DataValidator, CustomerModel, OrderModel, 
    ValidationRecord, UserPreferences, ValidationSummary
)
from type_checking import TypeSafetyChecker, PydanticTypeChecker
from decimal import Decimal

def demonstrate_type_safe_validation():
    """Show how the improved types provide better type safety"""
    print("=== Type-Safe Data Validation Examples ===")
    
    # Example 1: Using ValidationRecord instead of Dict[str, Any]
    customer_data: ValidationRecord = {
        'customer_id': 1,
        'name': 'Alice Johnson',
        'age': 28,
        'city': 'San Francisco', 
        'salary': 85000.0,
        'department': 'Engineering'
    }
    
    try:
        validated_customer = DataValidator.validate_customer_data(customer_data)
        print(f"[SUCCESS] Customer validated: {validated_customer.name} - {validated_customer.department}")
    except Exception as e:
        print(f"[ERROR] Validation failed: {e}")
    
    # Example 2: Using UserPreferences instead of Dict[str, Any]
    user_prefs: UserPreferences = {
        'theme': 'dark',
        'notifications': True,
        'max_items': 100,
        'timeout': 30.5
    }
    
    print(f"[SUCCESS] User preferences with proper typing: {user_prefs}")
    
    # Example 3: Batch validation with detailed type information
    batch_data: List[ValidationRecord] = [
        {'customer_id': 1, 'name': 'Bob', 'age': 25, 'salary': 50000, 'department': 'Sales'},
        {'customer_id': 2, 'name': 'Carol', 'age': 30, 'salary': 60000, 'department': 'Marketing'},
        {'customer_id': 3, 'name': '', 'age': -5, 'salary': -1000, 'department': 'InvalidDept'}  # Invalid
    ]
    
    summary: ValidationSummary = DataValidator.get_validation_summary(batch_data, CustomerModel)
    print(f"[SUCCESS] Batch validation summary:")
    print(f"   Valid records: {summary['valid_count']}")
    print(f"   Invalid records: {summary['invalid_count']}")
    print(f"   Success rate: {summary['validation_rate']:.1%}")

def demonstrate_runtime_type_checking():
    """Show runtime type checking capabilities"""
    print("\n=== Runtime Type Checking Examples ===")
    
    # Example 1: Basic type validation
    test_cases = [
        ("hello", str, "String"),
        (42, int, "Integer"),
        ([1, 2, 3], List[int], "List[int]"),
        ({'a': 1, 'b': 2}, Dict[str, int], "Dict[str, int]"),
        ("test", Union[str, int], "Union[str, int]"),
    ]
    
    for value, expected_type, description in test_cases:
        is_valid = TypeSafetyChecker.validate_type_annotation(value, expected_type)
        status = "[SUCCESS]" if is_valid else "[ERROR]"
        print(f"{status} {description}: {value} -> {is_valid}")
    
    # Example 2: Complex validation
    complex_data = {
        'user_id': 123,
        'preferences': {'theme': 'dark', 'notifications': True},
        'tags': ['python', 'pyspark', 'data']
    }
    
    ComplexType = Dict[str, Union[int, UserPreferences, List[str]]]
    is_valid = TypeSafetyChecker.validate_type_annotation(complex_data, ComplexType)
    print(f"[SUCCESS] Complex type validation: {is_valid}")

def demonstrate_enhanced_pydantic_validation():
    """Show enhanced Pydantic validation with detailed error reporting"""
    print("\n=== Enhanced Pydantic Validation Examples ===")
    
    # Test data with mixed valid/invalid records
    test_data = [
        {'customer_id': 1, 'name': 'Alice', 'age': 25, 'salary': 50000, 'department': 'Engineering'},
        {'customer_id': 2, 'name': 'Bob', 'age': 30, 'salary': 60000, 'department': 'Sales'},
        {'customer_id': -1, 'name': '', 'age': 16, 'salary': -5000, 'department': 'BadDept'},  # Multiple errors
    ]
    
    # Enhanced validation with detailed error reporting  
    results = PydanticTypeChecker.batch_validate_with_details(test_data, CustomerModel)
    
    print(f"[SUCCESS] Enhanced validation results:")
    print(f"   Valid records: {results['valid_count']}")
    print(f"   Error records: {results['error_count']}")
    print(f"   Success rate: {results['success_rate']:.1%}")
    
    if results['errors']:
        print(f"\n[DETAILS] Detailed error information:")
        for error in results['errors']:
            print(f"   [WARNING] {error}")

@TypeSafetyChecker.check_function_types
def type_checked_function(name: str, age: int, scores: List[float]) -> Dict[str, Union[str, int, float]]:
    """Example of a function with runtime type checking"""
    return {
        'name': name,
        'age': age,
        'average_score': sum(scores) / len(scores) if scores else 0.0,
        'max_score': max(scores) if scores else 0.0
    }

def demonstrate_function_type_checking():
    """Show function-level type checking"""
    print("\n=== Function-Level Type Checking Examples ===")
    
    try:
        # Valid call
        result = type_checked_function("Alice", 25, [85.5, 92.0, 78.5])
        print(f"[SUCCESS] Valid function call: {result}")
        
        # Invalid call (wrong type for age)
        # result = type_checked_function("Bob", "25", [85.5])  # Uncommenting this would raise TypeError
        
    except TypeError as e:
        print(f"[ERROR] Type checking caught error: {e}")

def main():
    """Run all type safety demonstrations"""
    print("Type Safety Improvements in PySpark Project")
    print("=" * 50)
    
    demonstrate_type_safe_validation()
    demonstrate_runtime_type_checking() 
    demonstrate_enhanced_pydantic_validation()
    demonstrate_function_type_checking()
    
    print("\n" + "=" * 50)
    print("[SUMMARY] Type Safety Summary:")
    print("[TIP] Replaced Dict[str, Any] with specific ValidationRecord type")
    print("[TIP] Added UserPreferences type for structured preferences")
    print("[TIP] Enhanced ValidationSummary with proper typing")
    print("[TIP] Added runtime type checking utilities")
    print("[TIP] Improved function decorators with ParamSpec and TypeVar")
    print("[TIP] Created comprehensive type validation tools")

if __name__ == "__main__":
    main()