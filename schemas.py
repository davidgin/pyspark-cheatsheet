from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, TimestampType, DoubleType
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession

class DataSchemas:
    """Centralized schema definitions for all datasets"""
    
    @staticmethod
    def customers_schema() -> StructType:
        """Schema for customers/employees data (sample_data.csv)"""
        return StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("city", StringType(), True),
            StructField("salary", DecimalType(10, 2), True),
            StructField("department", StringType(), True)
        ])
    
    @staticmethod
    def orders_schema() -> StructType:
        """Schema for orders data (orders.csv)"""
        return StructType([
            StructField("order_id", IntegerType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("product_name", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("price", DecimalType(10, 2), False),
            StructField("order_date", DateType(), True),
            StructField("status", StringType(), True)
        ])
    
    @staticmethod
    def user_activity_schema() -> StructType:
        """Schema for streaming user activity data"""
        return StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("user_id", IntegerType(), False),
            StructField("action", StringType(), False),
            StructField("product", StringType(), True),
            StructField("value", DecimalType(10, 2), True)
        ])
    
    @staticmethod
    def user_activity_raw_schema() -> StructType:
        """Schema for raw streaming user activity (string timestamps)"""
        return StructType([
            StructField("timestamp", StringType(), False),
            StructField("user_id", IntegerType(), False),
            StructField("action", StringType(), False),
            StructField("product", StringType(), True),
            StructField("value", DecimalType(10, 2), True)
        ])
    
    @staticmethod
    def transactions_schema() -> StructType:
        """Schema for transaction data (for windowed aggregation)"""
        return StructType([
            StructField("event_time", TimestampType(), False),
            StructField("user_id", IntegerType(), False),
            StructField("product_id", StringType(), False),
            StructField("amount", DecimalType(10, 2), False),
            StructField("city", StringType(), True)
        ])
    
    @staticmethod
    def transactions_raw_schema() -> StructType:
        """Schema for raw transaction data (string timestamps)"""
        return StructType([
            StructField("event_time", StringType(), False),
            StructField("user_id", IntegerType(), False),
            StructField("product_id", StringType(), False),
            StructField("amount", DecimalType(10, 2), False),
            StructField("city", StringType(), True)
        ])
    
    @staticmethod
    def live_data_schema():
        """Schema for live streaming data"""
        return StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("user_id", IntegerType(), False),
            StructField("action", StringType(), False),
            StructField("value", DecimalType(10, 2), True),
            StructField("session_id", StringType(), False)
        ])
    
    @staticmethod
    def live_data_raw_schema():
        """Schema for raw live streaming data (string timestamps)"""
        return StructType([
            StructField("timestamp", StringType(), False),
            StructField("user_id", IntegerType(), False),
            StructField("action", StringType(), False),
            StructField("value", DecimalType(10, 2), True),
            StructField("session_id", StringType(), False)
        ])
    
    @staticmethod
    def web_events_schema():
        """Schema for web events (JSON format)"""
        return StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("user_id", IntegerType(), False),
            StructField("session_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("page", StringType(), True),
            StructField("duration_ms", IntegerType(), True)
        ])
    
    @staticmethod
    def sales_data_schema():
        """Schema for sales/revenue data"""
        return StructType([
            StructField("sale_id", LongType(), False),
            StructField("product_id", StringType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("sale_date", DateType(), False),
            StructField("sale_timestamp", TimestampType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DecimalType(10, 2), False),
            StructField("total_amount", DecimalType(12, 2), False),
            StructField("discount", DecimalType(5, 2), True),
            StructField("region", StringType(), True),
            StructField("store_id", StringType(), True)
        ])
    
    @staticmethod
    def nested_data_schema():
        """Schema for complex nested data structures"""
        return StructType([
            StructField("id", IntegerType(), False),
            StructField("user_info", StructType([
                StructField("user_id", IntegerType(), False),
                StructField("name", StringType(), False),
                StructField("email", StringType(), True),
                StructField("preferences", StructType([
                    StructField("notifications", BooleanType(), True),
                    StructField("theme", StringType(), True),
                    StructField("language", StringType(), True)
                ]), True)
            ]), False),
            StructField("orders", ArrayType(StructType([
                StructField("order_id", IntegerType(), False),
                StructField("items", ArrayType(StructType([
                    StructField("product_id", StringType(), False),
                    StructField("quantity", IntegerType(), False),
                    StructField("price", DecimalType(10, 2), False)
                ])), False),
                StructField("order_date", TimestampType(), False),
                StructField("total", DecimalType(12, 2), False)
            ])), True),
            StructField("metadata", MapType(StringType(), StringType()), True)
        ])

class SchemaUtils:
    """Utility functions for working with schemas"""
    
    @staticmethod
    def print_schema_definition(schema: StructType, name: str = "Schema") -> None:
        """Print a readable schema definition"""
        print(f"\n=== {name} ===")
        print("StructType([")
        for field in schema.fields:
            nullable = "True" if field.nullable else "False"
            print(f"    StructField(\"{field.name}\", {field.dataType}, {nullable}),")
        print("])")
    
    @staticmethod
    def validate_dataframe_schema(df: DataFrame, expected_schema: StructType, name: str = "DataFrame") -> bool:
        """Validate that a DataFrame matches the expected schema"""
        print(f"\n=== Schema Validation for {name} ===")
        
        if df.schema == expected_schema:
            print("✅ Schema matches perfectly!")
            return True
        else:
            print("❌ Schema mismatch detected!")
            print("\nExpected Schema:")
            for field in expected_schema.fields:
                print(f"  {field.name}: {field.dataType} (nullable: {field.nullable})")
            print("\nActual Schema:")
            df.printSchema()
            return False
    
    @staticmethod
    def get_schema_for_file_type(file_type: str) -> Optional[StructType]:
        """Get appropriate schema based on file type"""
        schema_mapping: Dict[str, StructType] = {
            "customers": DataSchemas.customers_schema(),
            "orders": DataSchemas.orders_schema(),
            "user_activity": DataSchemas.user_activity_raw_schema(),
            "transactions": DataSchemas.transactions_raw_schema(),
            "live_data": DataSchemas.live_data_raw_schema(),
            "sales": DataSchemas.sales_data_schema(),
            "nested": DataSchemas.nested_data_schema()
        }
        
        return schema_mapping.get(file_type, None)
    
    @staticmethod
    def compare_schemas(schema1: StructType, schema2: StructType) -> Dict[str, List[str]]:
        """Compare two schemas and return differences"""
        differences = {
            "missing_fields": [],
            "extra_fields": [],
            "type_mismatches": [],
            "nullability_differences": []
        }
        
        schema1_fields = {field.name: field for field in schema1.fields}
        schema2_fields = {field.name: field for field in schema2.fields}
        
        # Find missing and extra fields
        for field_name in schema1_fields:
            if field_name not in schema2_fields:
                differences["missing_fields"].append(field_name)
        
        for field_name in schema2_fields:
            if field_name not in schema1_fields:
                differences["extra_fields"].append(field_name)
        
        # Check type and nullability differences
        for field_name in schema1_fields:
            if field_name in schema2_fields:
                field1 = schema1_fields[field_name]
                field2 = schema2_fields[field_name]
                
                if field1.dataType != field2.dataType:
                    differences["type_mismatches"].append(
                        f"{field_name}: {field1.dataType} vs {field2.dataType}"
                    )
                
                if field1.nullable != field2.nullable:
                    differences["nullability_differences"].append(
                        f"{field_name}: {field1.nullable} vs {field2.nullable}"
                    )
        
        return differences
    
    @staticmethod
    def create_dataframe_with_schema(spark: SparkSession, data: List[Dict], schema: StructType) -> DataFrame:
        """Create DataFrame with explicit schema and type validation"""
        try:
            # Convert data to proper format
            formatted_data = []
            for row in data:
                formatted_row = []
                for field in schema.fields:
                    value = row.get(field.name)
                    if value is None and not field.nullable:
                        raise ValueError(f"Non-nullable field '{field.name}' cannot be None")
                    formatted_row.append(value)
                formatted_data.append(formatted_row)
            
            return spark.createDataFrame(formatted_data, schema)
        except Exception as e:
            raise ValueError(f"Failed to create DataFrame with schema: {e}")

def main():
    """Demonstrate schema definitions"""
    print("PySpark Schema Definitions")
    print("=" * 40)
    
    # Print all available schemas
    SchemaUtils.print_schema_definition(DataSchemas.customers_schema(), "Customers Schema")
    SchemaUtils.print_schema_definition(DataSchemas.orders_schema(), "Orders Schema")
    SchemaUtils.print_schema_definition(DataSchemas.user_activity_schema(), "User Activity Schema")
    SchemaUtils.print_schema_definition(DataSchemas.transactions_schema(), "Transactions Schema")
    SchemaUtils.print_schema_definition(DataSchemas.nested_data_schema(), "Nested Data Schema")
    
    print("\n" + "=" * 40)
    print("Schema definitions ready for use!")
    print("Import with: from schemas import DataSchemas, SchemaUtils")

if __name__ == "__main__":
    main()