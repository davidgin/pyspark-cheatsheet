from pyspark.sql import SparkSession
from schemas import DataSchemas
from models import CustomerModel
from models import DataValidationError
from models import DataValidator
from models import OrderModel
from models import UserActivityModel
from decimal import Decimal
from datetime import date, datetime

def create_spark_session() -> SparkSession:
    """Create Spark session for Pydantic examples"""
    return SparkSession.builder \
        .appName("PySpark Pydantic Integration Examples") \
        .master("local[*]") \
        .getOrCreate()

def example_1_basic_validation() -> None:
    """Example 1: Basic Pydantic validation"""
    print("=== EXAMPLE 1: Basic Pydantic Validation ===")
    
    # Valid customer data
    valid_customer = {
        "customer_id": 1,
        "name": "  alice johnson  ",  # Will be cleaned and capitalized
        "age": 25,
        "city": "new york",  # Will be capitalized
        "salary": Decimal("75000.50"),
        "department": "Engineering"
    }
    
    try:
        validated = DataValidator.validate_customer_data(valid_customer)
        print(f"[SUCCESS] Valid customer: {validated.name} in {validated.city}")
        print(f"   Cleaned data: {validated.json()}")
    except DataValidationError as e:
        print(f"[ERROR] Validation failed: {e}")
    
    # Invalid customer data (missing required field)
    invalid_customer = {
        "name": "",  # Empty name will fail validation
        "age": 25,
        "salary": -1000  # Negative salary will fail
    }
    
    try:
        validated = DataValidator.validate_customer_data(invalid_customer)
        print(f"[ERROR] Should have failed: {validated}")
    except DataValidationError as e:
        print(f"[SUCCESS] Correctly caught invalid data: {e}")

def example_2_business_rules_validation() -> None:
    """Example 2: Business rules validation"""
    print("\n=== EXAMPLE 2: Business Rules Validation ===")
    
    # Order that exceeds business rule limit
    large_order = {
        "order_id": 1001,
        "customer_id": 1,
        "product_name": "  expensive laptop  ",
        "quantity": 10,
        "price": Decimal("1200.00"),  # Total: $12,000 > $10,000 limit
        "status": "completed"
    }
    
    try:
        validated = DataValidator.validate_order_data(large_order)
        print(f"[ERROR] Should have failed: {validated}")
    except DataValidationError as e:
        print(f"[SUCCESS] Business rule enforced: {e}")
    
    # Valid order within limits
    valid_order = {
        "order_id": 1002,
        "customer_id": 1,
        "product_name": "laptop",
        "quantity": 2,
        "price": Decimal("1200.00"),  # Total: $2,400 within limit
        "order_date": date(2024, 1, 15),
        "status": "completed"
    }
    
    try:
        validated = DataValidator.validate_order_data(valid_order)
        print(f"[SUCCESS] Valid order: {validated.product_name} x{validated.quantity} = ${validated.quantity * validated.price}")
    except DataValidationError as e:
        print(f"[ERROR] Unexpected validation error: {e}")

def example_3_batch_validation() -> None:
    """Example 3: Batch data validation"""
    print("\n=== EXAMPLE 3: Batch Data Validation ===")
    
    # Mixed batch of customer data (some valid, some invalid)
    customer_batch = [
        {"customer_id": 1, "name": "Alice", "age": 25, "city": "NYC", "salary": 75000, "department": "Engineering"},
        {"customer_id": 2, "name": "Bob", "age": 30, "city": "SF", "salary": 85000, "department": "Sales"},
        {"customer_id": 3, "name": "", "age": -5, "salary": -1000, "department": "Invalid"},  # Invalid record
        {"customer_id": 4, "name": "Diana", "age": 28, "city": "Boston", "salary": 70000, "department": "HR"},
    ]
    
    # Get validation summary (doesn't raise exceptions)
    summary = DataValidator.get_validation_summary(customer_batch, CustomerModel)
    
    print(f"[SUMMARY] Validation Summary:")
    print(f"   Total records: {summary['total_records']}")
    print(f"   Valid records: {summary['valid_count']}")
    print(f"   Invalid records: {summary['invalid_count']}")
    print(f"   Success rate: {summary['validation_rate']:.1%}")
    
    if summary['invalid_records']:
        print(f"   First invalid record: {summary['invalid_records'][0][1]}")
    
    # Extract only valid records
    valid_records = [record for i, record in summary['valid_records']]
    print(f"[SUCCESS] Successfully validated {len(valid_records)} customers")
    
    # Try strict batch validation (will raise exception)
    try:
        strict_validation = DataValidator.validate_batch_data(customer_batch, CustomerModel)
        print(f"[ERROR] Should have failed due to invalid records")
    except DataValidationError as e:
        print(f"[SUCCESS] Strict validation correctly failed: {str(e)[:100]}...")

def example_4_enum_validation() -> None:
    """Example 4: Enum validation and type safety"""
    print("\n=== EXAMPLE 4: Enum Validation ===")
    
    # Valid enum values
    valid_enums = [
        {"customer_id": 1, "name": "Alice", "department": "Engineering"},
        {"order_id": 1, "customer_id": 1, "product_name": "Laptop", "quantity": 1, "price": 1000, "status": "completed"},
        {"timestamp": datetime.now(), "user_id": 1, "action": "purchase", "product": "laptop", "value": 1000}
    ]
    
    models_to_test = [
        (CustomerModel, valid_enums[0]),
        (OrderModel, valid_enums[1]),
        (UserActivityModel, valid_enums[2])
    ]
    
    for model_class, data in models_to_test:
        try:
            validated = model_class(**data)
            print(f"[SUCCESS] {model_class.__name__} validation successful")
        except Exception as e:
            print(f"[ERROR] {model_class.__name__} validation failed: {e}")
    
    # Invalid enum values
    print("\n--- Testing Invalid Enum Values ---")
    invalid_enum_data = [
        {"customer_id": 1, "name": "Alice", "department": "InvalidDept"},
        {"order_id": 1, "customer_id": 1, "product_name": "Laptop", "quantity": 1, "price": 1000, "status": "invalid_status"},
        {"timestamp": datetime.now(), "user_id": 1, "action": "invalid_action", "value": 1000}
    ]
    
    for i, (model_class, data) in enumerate(zip([CustomerModel, OrderModel, UserActivityModel], invalid_enum_data)):
        try:
            validated = model_class(**data)
            print(f"[ERROR] {model_class.__name__} should have failed")
        except Exception as e:
            print(f"[SUCCESS] {model_class.__name__} correctly rejected invalid enum: {str(e)[:60]}...")

def example_5_spark_integration() -> None:
    """Example 5: Integration with PySpark DataFrames"""
    print("\n=== EXAMPLE 5: PySpark Integration ===")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Create DataFrame with explicit schema
        customers_schema = DataSchemas.customers_schema()
        
        # Sample data that matches our Pydantic models
        sample_data = [
            (1, "Alice Johnson", 25, "New York", Decimal("75000.00"), "Engineering"),
            (2, "Bob Smith", 30, "San Francisco", Decimal("85000.00"), "Sales"),
            (3, "Charlie Brown", 35, "Chicago", Decimal("65000.00"), "Marketing"),
        ]
        
        df = spark.createDataFrame(sample_data, customers_schema)
        
        print("[SCHEMA] DataFrame Schema:")
        df.printSchema()
        
        print("\n[DATA] DataFrame Data:")
        df.show()
        
        # Convert DataFrame rows to Pydantic models for validation
        print("\n[VALIDATING] Validating DataFrame rows with Pydantic:")
        
        rows = df.collect()
        for row in rows:
            row_dict = row.asDict()
            try:
                validated_customer = CustomerModel(**row_dict)
                print(f"[SUCCESS] Row validated: {validated_customer.name} - {validated_customer.department}")
            except Exception as e:
                print(f"[ERROR] Row validation failed: {e}")
        
        # Demonstrate validation summary on DataFrame data
        df_data = [row.asDict() for row in df.collect()]
        validation_summary = DataValidator.get_validation_summary(df_data, CustomerModel)
        
        print(f"\n[SUMMARY] DataFrame Validation Summary:")
        print(f"   Spark DataFrame rows: {df.count()}")
        print(f"   Pydantic valid rows: {validation_summary['valid_count']}")
        print(f"   Validation success rate: {validation_summary['validation_rate']:.1%}")
        
    finally:
        spark.stop()

def example_6_data_cleaning_pipeline() -> None:
    """Example 6: Data cleaning pipeline with Pydantic"""
    print("\n=== EXAMPLE 6: Data Cleaning Pipeline ===")
    
    # Raw data that needs cleaning
    raw_customer_data = [
        {"customer_id": 1, "name": "  ALICE JOHNSON  ", "city": "new york", "department": "engineering"},
        {"customer_id": 2, "name": "bob smith", "city": "  San Francisco  ", "department": "Sales"},
        {"customer_id": 3, "name": "Charlie", "city": "CHICAGO", "department": "marketing"},
    ]
    
    print("[RAW DATA] Raw Data (before cleaning):")
    for data in raw_customer_data:
        print(f"   {data}")
    
    # Clean data using Pydantic validators
    cleaned_data = []
    for raw_data in raw_customer_data:
        try:
            # Add required fields with defaults
            raw_data.update({"age": 30, "salary": 50000})  # Add missing required fields
            
            cleaned_customer = CustomerModel(**raw_data)
            cleaned_data.append(cleaned_customer)
        except Exception as e:
            print(f"[WARNING] Skipping invalid record: {e}")
    
    print(f"\n[CLEANED] Cleaned Data ({len(cleaned_data)} records):")
    for customer in cleaned_data:
        print(f"   {customer.name} | {customer.city} | {customer.department}")
    
    # Show JSON output
    print(f"\n[JSON] JSON Export:")
    for customer in cleaned_data:
        print(f"   {customer.json()}")

def main() -> None:
    """Run all Pydantic integration examples"""
    print("PySpark + Pydantic Integration Examples")
    print("=" * 50)
    
    try:
        example_1_basic_validation()
        example_2_business_rules_validation()
        example_3_batch_validation()
        example_4_enum_validation()
        example_5_spark_integration()
        example_6_data_cleaning_pipeline()
        
        print("\n" + "=" * 50)
        print("[COMPLETE] Pydantic Integration Examples Complete!")
        print("\nKey Benefits Demonstrated:")
        print("[SUCCESS] Runtime data validation")
        print("[SUCCESS] Automatic data cleaning and formatting")
        print("[SUCCESS] Business rule enforcement")
        print("[SUCCESS] Type safety with enums")
        print("[SUCCESS] Batch validation capabilities")
        print("[SUCCESS] Seamless PySpark integration")
        
    except Exception as e:
        print(f"[ERROR] Error in examples: {e}")

if __name__ == "__main__":
    main()