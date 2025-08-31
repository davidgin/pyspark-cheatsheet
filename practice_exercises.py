from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, round, sum, to_date, when
# No explicit types used in this file
from schemas import DataSchemas, SchemaUtils

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("PySpark Practice Exercises") \
        .master("local[*]") \
        .getOrCreate()

def exercise_1_basic_operations(spark: SparkSession) -> None:
    """Exercise 1: Basic DataFrame operations with explicit schema"""
    print("=== EXERCISE 1: Basic Operations ===")
    
    # Load data with explicit schema
    schema = DataSchemas.customers_schema()
    customers_df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv("data/sample_data.csv")
    
    print("Schema being used:")
    SchemaUtils.print_schema_definition(schema, "Customers Schema")
    
    print("\nTODO: Complete these exercises:")
    print("1. Show only customers from Engineering department")
    print("2. Filter customers with salary > 70000")
    print("3. Add a new column 'salary_category' (High/Medium/Low based on salary)")
    print("4. Practice working with DecimalType for salary calculations")
    
    # Solution examples (uncomment to see results):
    # customers_df.filter(col("department") == "Engineering").show()
    # customers_df.filter(col("salary") > 70000).show()
    # customers_df.withColumn("salary_category", 
    #     when(col("salary") > 80000, "High")
    #     .when(col("salary") > 65000, "Medium")
    #     .otherwise("Low")).show()
    #
    # # Working with DecimalType
    # customers_df.withColumn("annual_bonus", col("salary") * 0.15) \
    #     .withColumn("total_compensation", col("salary") + col("annual_bonus")) \
    #     .select("name", "salary", "annual_bonus", "total_compensation").show()

def exercise_2_aggregations(spark: SparkSession) -> None:
    """Exercise 2: Aggregations and grouping with explicit schema"""
    print("\n=== EXERCISE 2: Aggregations ===")
    
    # Load with explicit schema
    schema = DataSchemas.orders_schema()
    orders_df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv("data/orders.csv") \
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    
    print("Schema being used:")
    SchemaUtils.print_schema_definition(schema, "Orders Schema")
    
    print("\nTODO: Complete these exercises:")
    print("1. Find total revenue by product")
    print("2. Calculate average order value by status")
    print("3. Find the top 3 customers by total spending")
    print("4. Practice DecimalType aggregations")
    
    # Solution examples:
    # orders_df.withColumn("revenue", col("quantity") * col("price")) \
    #     .groupBy("product_name").agg(sum("revenue").alias("total_revenue")) \
    #     .orderBy("total_revenue", ascending=False).show()
    #
    # # Working with DecimalType precision
    # orders_df.groupBy("status") \
    #     .agg(
    #         avg("price").alias("avg_price"),
    #         sum(col("quantity") * col("price")).alias("total_revenue"),
    #         count("*").alias("order_count"),
    #         round(avg(col("quantity") * col("price")), 2).alias("avg_order_value")
    #     ).show()

def exercise_3_joins(spark: SparkSession) -> None:
    """Exercise 3: Join operations with explicit schemas"""
    print("\n=== EXERCISE 3: Joins ===")
    
    # Load both datasets with explicit schemas
    customers_schema = DataSchemas.customers_schema()
    orders_schema = DataSchemas.orders_schema()
    
    customers_df = spark.read \
        .option("header", "true") \
        .schema(customers_schema) \
        .csv("data/sample_data.csv")
    
    orders_df = spark.read \
        .option("header", "true") \
        .schema(orders_schema) \
        .csv("data/orders.csv") \
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    
    print("Using explicit schemas for both datasets")
    print("Customers schema:", customers_schema.fieldNames())
    print("Orders schema:", orders_schema.fieldNames())
    
    print("\nTODO: Complete these exercises:")
    print("1. Inner join customers and orders")
    print("2. Left join to show customers who haven't ordered")
    print("3. Find which department spends the most on orders")
    print("4. Practice schema-aware joins")
    
    # Solution examples:
    # # Inner join
    # joined_df = customers_df.join(orders_df, "customer_id", "inner")
    # joined_df.select("name", "department", "product_name", "quantity", "price").show()
    #
    # # Left join to find customers without orders
    # left_join_df = customers_df.join(orders_df, "customer_id", "left")
    # customers_without_orders = left_join_df.filter(col("order_id").isNull())
    # customers_without_orders.select("name", "department", "city").show()
    #
    # # Department spending analysis
    # dept_spending = joined_df.groupBy("department") \
    #     .agg(sum(col("quantity") * col("price")).alias("total_spending")) \
    #     .orderBy("total_spending", ascending=False)
    # dept_spending.show()

def exercise_4_window_functions(spark: SparkSession) -> None:
    """Exercise 4: Window functions"""
    print("\n=== EXERCISE 4: Window Functions ===")
    
    orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")
    
    print("TODO: Complete these exercises:")
    print("1. Rank orders by price within each status")
    print("2. Calculate running total of revenue")
    print("3. Find the difference between each order and the previous order price")
    
    # Hint: Use Window.partitionBy() and Window.orderBy()

def exercise_5_sql_practice(spark: SparkSession) -> None:
    """Exercise 5: SQL queries"""
    print("\n=== EXERCISE 5: SQL Practice ===")
    
    customers_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/sample_data.csv")
    orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")
    
    customers_df.createOrReplaceTempView("customers")
    orders_df.createOrReplaceTempView("orders")
    
    print("TODO: Write SQL queries for:")
    print("1. Find customers who have placed more than 2 orders")
    print("2. Calculate monthly order totals")
    print("3. Find the most popular product by quantity sold")
    
    # Example query:
    # result = spark.sql("""
    #     SELECT customer_id, COUNT(*) as order_count
    #     FROM orders 
    #     GROUP BY customer_id 
    #     HAVING COUNT(*) > 2
    # """)
    # result.show()

def exercise_6_data_cleaning(spark: SparkSession) -> None:
    """Exercise 6: Data cleaning and transformation"""
    print("\n=== EXERCISE 6: Data Cleaning ===")
    
    print("TODO: Complete these data cleaning tasks:")
    print("1. Handle null values in the datasets")
    print("2. Standardize city names (remove extra spaces, capitalize)")
    print("3. Validate email addresses (add a validation column)")
    print("4. Convert date strings to proper date format")

def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Welcome to PySpark Practice Exercises!")
    print("="*50)
    
    try:
        exercise_1_basic_operations(spark)
        exercise_2_aggregations(spark)
        exercise_3_joins(spark)
        exercise_4_window_functions(spark)
        exercise_5_sql_practice(spark)
        exercise_6_data_cleaning(spark)
        
        print("\n" + "="*50)
        print("Practice exercises loaded! Uncomment solutions to see results.")
        print("Happy PySpark learning!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()