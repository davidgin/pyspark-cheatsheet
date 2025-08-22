from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("PySpark Windowing and Watermarking Examples") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoints") \
        .getOrCreate()

def example_1_tumbling_windows():
    """Example 1: Tumbling windows (non-overlapping)"""
    print("=== EXAMPLE 1: Tumbling Windows ===")
    print("Non-overlapping 5-minute windows")
    
    spark = create_spark_session()
    
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True)
    ])
    
    # Read streaming data
    df = spark \
        .readStream \
        .option("header", "true") \
        .schema(schema) \
        .csv("streaming_data")
    
    # Convert to timestamp and apply tumbling windows
    windowed_df = df \
        .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")) \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes"),  # 5-minute tumbling windows
            col("product_id")
        ) \
        .agg(
            sum("amount").alias("total_sales"),
            count("*").alias("transaction_count"),
            avg("amount").alias("avg_transaction")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("product_id"),
            round(col("total_sales"), 2).alias("total_sales"),
            col("transaction_count"),
            round(col("avg_transaction"), 2).alias("avg_transaction")
        ) \
        .orderBy("window_start", "total_sales")
    
    query = windowed_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='8 seconds') \
        .start()
    
    print("Tumbling windows running for 40 seconds...")
    try:
        query.awaitTermination(timeout=40)
    finally:
        query.stop()
        spark.stop()

def example_2_sliding_windows():
    """Example 2: Sliding windows (overlapping)"""
    print("\n=== EXAMPLE 2: Sliding Windows ===")
    print("10-minute windows sliding every 5 minutes")
    
    spark = create_spark_session()
    
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("city", StringType(), True)
    ])
    
    # Read streaming data
    df = spark \
        .readStream \
        .option("header", "true") \
        .schema(schema) \
        .csv("streaming_data")
    
    # Apply sliding windows
    windowed_df = df \
        .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")) \
        .withWatermark("event_time", "15 minutes") \
        .groupBy(
            window(col("event_time"), "10 minutes", "5 minutes"),  # 10-min window, 5-min slide
            col("city")
        ) \
        .agg(
            sum("amount").alias("revenue"),
            count("*").alias("orders"),
            countDistinct("user_id").alias("unique_customers"),
            countDistinct("product_id").alias("unique_products")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("city"),
            round(col("revenue"), 2).alias("revenue"),
            col("orders"),
            col("unique_customers"),
            col("unique_products")
        ) \
        .orderBy("window_start", "revenue")
    
    query = windowed_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print("Sliding windows running for 50 seconds...")
    try:
        query.awaitTermination(timeout=50)
    finally:
        query.stop()
        spark.stop()

def example_3_watermarking_late_data():
    """Example 3: Watermarking to handle late-arriving data"""
    print("\n=== EXAMPLE 3: Watermarking for Late Data ===")
    print("Demonstrates how watermarking handles late events")
    
    spark = create_spark_session()
    
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("value", DoubleType(), True)
    ])
    
    # Read streaming data
    df = spark \
        .readStream \
        .option("header", "true") \
        .schema(schema) \
        .csv("streaming_data")
    
    # Apply watermarking with different thresholds
    windowed_df = df \
        .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("processing_time", current_timestamp()) \
        .withWatermark("event_time", "5 minutes") \
        .groupBy(
            window(col("event_time"), "3 minutes"),
            col("action")
        ) \
        .agg(
            count("*").alias("action_count"),
            sum("value").alias("total_value"),
            min("event_time").alias("earliest_event"),
            max("event_time").alias("latest_event")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("action"),
            col("action_count"),
            round(col("total_value"), 2).alias("total_value"),
            col("earliest_event"),
            col("latest_event")
        ) \
        .withColumn("watermark_info", 
                   concat(lit("Late threshold: 5 min, Window: 3 min")))
    
    query = windowed_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='6 seconds') \
        .start()
    
    print("Watermarking example running for 35 seconds...")
    try:
        query.awaitTermination(timeout=35)
    finally:
        query.stop()
        spark.stop()

def example_4_session_windows():
    """Example 4: Session windows (time-based sessions)"""
    print("\n=== EXAMPLE 4: Session Windows ===")
    print("Groups events by user sessions with 10-minute timeout")
    
    spark = create_spark_session()
    
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("session_id", StringType(), True)
    ])
    
    # Read streaming data
    df = spark \
        .readStream \
        .option("header", "true") \
        .schema(schema) \
        .csv("streaming_data")
    
    # Session-based windowing
    session_df = df \
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            col("user_id"),
            session_window(col("timestamp"), "10 minutes")  # Session timeout of 10 minutes
        ) \
        .agg(
            count("*").alias("events_in_session"),
            sum("value").alias("session_value"),
            countDistinct("action").alias("unique_actions"),
            min("timestamp").alias("session_start"),
            max("timestamp").alias("session_end")
        ) \
        .withColumn("session_duration_minutes",
                   round((unix_timestamp("session_end") - unix_timestamp("session_start")) / 60, 2)) \
        .select(
            col("user_id"),
            col("session.start").alias("session_window_start"),
            col("session.end").alias("session_window_end"),
            col("events_in_session"),
            round(col("session_value"), 2).alias("session_value"),
            col("unique_actions"),
            col("session_start"),
            col("session_end"),
            col("session_duration_minutes")
        )
    
    query = session_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='12 seconds') \
        .start()
    
    print("Session windows running for 45 seconds...")
    try:
        query.awaitTermination(timeout=45)
    finally:
        query.stop()
        spark.stop()

def example_5_advanced_windowing():
    """Example 5: Advanced windowing with multiple aggregations"""
    print("\n=== EXAMPLE 5: Advanced Multi-level Windowing ===")
    print("Combines multiple window types for comprehensive analysis")
    
    spark = create_spark_session()
    
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("city", StringType(), True)
    ])
    
    # Read streaming data
    df = spark \
        .readStream \
        .option("header", "true") \
        .schema(schema) \
        .csv("streaming_data")
    
    timestamped_df = df \
        .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")) \
        .withWatermark("event_time", "15 minutes")
    
    # Multi-level windowing: Hourly rollups with 15-minute updates
    hourly_stats = timestamped_df \
        .groupBy(
            window(col("event_time"), "1 hour", "15 minutes"),
            col("city")
        ) \
        .agg(
            sum("amount").alias("hourly_revenue"),
            count("*").alias("hourly_transactions"),
            countDistinct("user_id").alias("hourly_unique_users"),
            countDistinct("product_id").alias("hourly_unique_products"),
            avg("amount").alias("hourly_avg_transaction"),
            min("amount").alias("hourly_min_transaction"),
            max("amount").alias("hourly_max_transaction")
        ) \
        .select(
            col("window.start").alias("hour_start"),
            col("window.end").alias("hour_end"),
            col("city"),
            round(col("hourly_revenue"), 2).alias("revenue"),
            col("hourly_transactions").alias("transactions"),
            col("hourly_unique_users").alias("unique_users"),
            col("hourly_unique_products").alias("unique_products"),
            round(col("hourly_avg_transaction"), 2).alias("avg_transaction"),
            round(col("hourly_min_transaction"), 2).alias("min_transaction"),
            round(col("hourly_max_transaction"), 2).alias("max_transaction")
        ) \
        .orderBy("hour_start", "revenue")
    
    query = hourly_stats.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 20) \
        .trigger(processingTime='15 seconds') \
        .start()
    
    print("Advanced windowing running for 60 seconds...")
    try:
        query.awaitTermination(timeout=60)
    finally:
        query.stop()
        spark.stop()

def run_windowing_example(example_number):
    """Run a specific windowing example"""
    examples = {
        1: example_1_tumbling_windows,
        2: example_2_sliding_windows,
        3: example_3_watermarking_late_data,
        4: example_4_session_windows,
        5: example_5_advanced_windowing
    }
    
    if example_number in examples:
        examples[example_number]()
    else:
        print(f"Example {example_number} not found")

def main():
    print("PySpark Windowing and Watermarking Examples")
    print("=" * 50)
    print("\nAvailable examples:")
    print("1. Tumbling Windows (non-overlapping)")
    print("2. Sliding Windows (overlapping)")
    print("3. Watermarking for Late Data")
    print("4. Session Windows")
    print("5. Advanced Multi-level Windowing")
    
    print("\nNote: Make sure to generate streaming data first:")
    print("python stream_data_generator.py")
    
    try:
        choice = int(input("\nEnter example number (1-5): "))
        print(f"\nRunning Windowing Example {choice}...")
        run_windowing_example(choice)
        
    except ValueError:
        print("Please enter a valid number (1-5)")
    except KeyboardInterrupt:
        print("\nExample interrupted by user")
    except Exception as e:
        print(f"Error running example: {e}")

if __name__ == "__main__":
    main()