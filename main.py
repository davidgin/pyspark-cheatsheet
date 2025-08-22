from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max, min as spark_min, to_date
from schemas import DataSchemas, SchemaUtils
from models import DataValidator

def create_spark_session() -> SparkSession:
    """Create and configure Spark session for local development"""
    return SparkSession.builder \
        .appName("PySpark Practice Project") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def load_customers_data(spark: SparkSession) -> DataFrame:
    """Load customers data from CSV file with explicit schema and validation"""
    schema = DataSchemas.customers_schema()
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv("data/sample_data.csv")
    
    # Validate schema
    is_valid = SchemaUtils.validate_dataframe_schema(df, schema, "Customers Data")
    
    if is_valid:
        print("✅ Customer data schema validation successful")
        # Sample Pydantic validation on first row
        sample_row = df.first().asDict()
        try:
            validated_customer = DataValidator.validate_customer_data(sample_row)
            print(f"✅ Pydantic validation sample: {validated_customer.name} in {validated_customer.department}")
        except Exception as e:
            print(f"⚠️  Pydantic validation warning: {e}")
    
    return df

def load_orders_data(spark: SparkSession) -> DataFrame:
    """Load orders data from CSV file with explicit schema and validation"""
    schema = DataSchemas.orders_schema()
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv("data/orders.csv")
    
    # Convert string date to proper date type
    df = df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    
    # Validate schema after transformation
    print(f"\n✅ Orders data loaded with explicit schema:")
    df.printSchema()
    
    # Sample Pydantic validation
    sample_row = df.first().asDict()
    try:
        validated_order = DataValidator.validate_order_data(sample_row)
        print(f"✅ Pydantic validation sample: Order {validated_order.order_id} for {validated_order.product_name}")
    except Exception as e:
        print(f"⚠️  Pydantic validation warning: {e}")
    
    return df

def analyze_customers(customers_df):
    """Analyze customers data"""
    print("=== CUSTOMERS DATA ANALYSIS ===")
    print(f"Total customers: {customers_df.count()}")
    
    print("\nCustomers Schema:")
    customers_df.printSchema()
    
    print("\nFirst 10 customers:")
    customers_df.show(10)
    
    print("\nSalary statistics by department:")
    customers_df.groupBy("department") \
        .agg(avg("salary").alias("avg_salary"),
             spark_max("salary").alias("max_salary"),
             spark_min("salary").alias("min_salary"),
             count("*").alias("employee_count")) \
        .orderBy("avg_salary", ascending=False) \
        .show()

def analyze_orders(orders_df):
    """Analyze orders data"""
    print("\n=== ORDERS DATA ANALYSIS ===")
    print(f"Total orders: {orders_df.count()}")
    
    print("\nOrders Schema:")
    orders_df.printSchema()
    
    print("\nFirst 10 orders:")
    orders_df.show(10)
    
    print("\nOrder statistics by status:")
    orders_df.groupBy("status") \
        .agg(count("*").alias("order_count"),
             spark_sum("quantity").alias("total_quantity"),
             avg("price").alias("avg_price")) \
        .show()

def join_analysis(customers_df, orders_df):
    """Perform join analysis between customers and orders"""
    print("\n=== JOIN ANALYSIS ===")
    
    joined_df = customers_df.join(orders_df, "customer_id", "inner")
    
    print("Customer order summary:")
    joined_df.groupBy("name", "department") \
        .agg(count("order_id").alias("total_orders"),
             spark_sum("quantity").alias("total_items"),
             spark_sum(col("quantity") * col("price")).alias("total_spent")) \
        .orderBy("total_spent", ascending=False) \
        .show()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("Loading datasets...")
        customers_df = load_customers_data(spark)
        orders_df = load_orders_data(spark)
        
        # Register as temporary tables for SQL practice
        customers_df.createOrReplaceTempView("customers")
        orders_df.createOrReplaceTempView("orders")
        
        print("Analyzing customers...")
        analyze_customers(customers_df)
        
        print("Analyzing orders...")
        analyze_orders(orders_df)
        
        print("Performing join analysis...")
        join_analysis(customers_df, orders_df)
        
        print("\n=== SQL EXAMPLE ===")
        sql_result = spark.sql("""
            SELECT c.name, c.department, COUNT(o.order_id) as order_count
            FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.name, c.department
            ORDER BY order_count DESC
        """)
        sql_result.show()
        
        print("\nPySpark analysis completed successfully!")
        print("Tables 'customers' and 'orders' are registered for SQL practice.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()