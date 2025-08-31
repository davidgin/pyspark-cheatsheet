from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, countDistinct, current_timestamp, hour, max, min, sum, to_timestamp, unix_timestamp, window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
# Most streaming functionality is accessed through DataFrame methods

def create_streaming_session():
    return SparkSession.builder \
        .appName("PySpark Streaming Examples") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoints") \
        .getOrCreate()

def example_1_basic_file_stream():
    """Example 1: Basic file streaming with transformations"""
    print("=== EXAMPLE 1: Basic File Streaming ===")
    
    spark = create_streaming_session()
    
    # Define schema for incoming data
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("product", StringType(), True),
        StructField("value", DoubleType(), True)
    ])
    
    # Read streaming data
    streaming_df = spark \
        .readStream \
        .option("header", "true") \
        .schema(schema) \
        .csv("streaming_data")
    
    # Transform data
    processed_df = streaming_df \
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("processing_time", current_timestamp()) \
        .filter(col("action").isin("purchase", "add_to_cart")) \
        .withColumn("hour", hour(col("timestamp")))
    
    # Write to console
    query = processed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='5 seconds') \
        .start()
    
    print("Streaming query started. Monitoring for 30 seconds...")
    try:
        query.awaitTermination(timeout=30)
    finally:
        query.stop()
        spark.stop()

def example_2_windowed_aggregation():
    """Example 2: Windowed aggregation with watermarking"""
    print("\n=== EXAMPLE 2: Windowed Aggregation ===")
    
    spark = create_streaming_session()
    
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("city", StringType(), True)
    ])
    
    # Read streaming data
    streaming_df = spark \
        .readStream \
        .option("header", "true") \
        .schema(schema) \
        .csv("streaming_data")
    
    # Convert to timestamp and apply watermarking
    timestamped_df = streaming_df \
        .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")) \
        .withWatermark("event_time", "10 minutes")
    
    # Windowed aggregation
    windowed_counts = timestamped_df \
        .groupBy(
            window(col("event_time"), "5 minutes", "5 minutes"),
            col("city")
        ) \
        .agg(
            sum("amount").alias("total_revenue"),
            count("*").alias("transaction_count"),
            avg("amount").alias("avg_transaction_value")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("city"),
            col("total_revenue"),
            col("transaction_count"),
            col("avg_transaction_value")
        )
    
    # Output results
    query = windowed_counts.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print("Windowed aggregation started. Running for 45 seconds...")
    try:
        query.awaitTermination(timeout=45)
    finally:
        query.stop()
        spark.stop()

def example_3_multiple_outputs():
    """Example 3: Multiple output sinks"""
    print("\n=== EXAMPLE 3: Multiple Output Sinks ===")
    
    spark = create_streaming_session()
    
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("session_id", StringType(), True)
    ])
    
    # Read streaming data
    streaming_df = spark \
        .readStream \
        .option("header", "true") \
        .schema(schema) \
        .csv("streaming_data")
    
    # Process data
    processed_df = streaming_df \
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("is_high_value", col("value") > 50)
    
    # Sink 1: All data to parquet files
    raw_query = processed_df.writeStream \
        .format("parquet") \
        .option("path", "output/streaming_raw") \
        .option("checkpointLocation", "checkpoints/raw_sink") \
        .outputMode("append") \
        .start()
    
    # Sink 2: High-value transactions to console
    high_value_df = processed_df.filter(col("is_high_value") == True)
    
    console_query = high_value_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .trigger(processingTime='8 seconds') \
        .start()
    
    # Sink 3: User activity summary
    user_summary = processed_df \
        .groupBy("user_id") \
        .agg(
            count("*").alias("total_actions"),
            sum("value").alias("total_value"),
            countDistinct("session_id").alias("unique_sessions")
        )
    
    summary_query = user_summary.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("numRows", 10) \
        .trigger(processingTime='15 seconds') \
        .start()
    
    print("Multiple sinks started. Running for 60 seconds...")
    
    try:
        # Run all queries concurrently
        raw_query.awaitTermination(timeout=60)
    finally:
        raw_query.stop()
        console_query.stop()
        summary_query.stop()
        spark.stop()

def example_4_stateful_operations():
    """Example 4: Stateful operations and session tracking"""
    print("\n=== EXAMPLE 4: Stateful Operations ===")
    
    spark = create_streaming_session()
    
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("session_id", StringType(), True)
    ])
    
    # Read streaming data
    streaming_df = spark \
        .readStream \
        .option("header", "true") \
        .schema(schema) \
        .csv("streaming_data")
    
    # Convert timestamp
    timestamped_df = streaming_df \
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    
    # Session-based aggregation (stateful)
    session_stats = timestamped_df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            col("user_id"),
            col("session_id"),
            window(col("timestamp"), "30 minutes", "30 minutes")
        ) \
        .agg(
            count("*").alias("actions_in_session"),
            sum("value").alias("session_value"),
            min("timestamp").alias("session_start"),
            max("timestamp").alias("session_end")
        ) \
        .withColumn("session_duration_minutes",
                   (unix_timestamp("session_end") - unix_timestamp("session_start")) / 60)
    
    # Output session statistics
    query = session_stats.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='12 seconds') \
        .start()
    
    print("Stateful session tracking started. Running for 50 seconds...")
    try:
        query.awaitTermination(timeout=50)
    finally:
        query.stop()
        spark.stop()

def run_example(example_number):
    """Run a specific example"""
    examples = {
        1: example_1_basic_file_stream,
        2: example_2_windowed_aggregation,
        3: example_3_multiple_outputs,
        4: example_4_stateful_operations
    }
    
    if example_number in examples:
        examples[example_number]()
    else:
        print(f"Example {example_number} not found")

def main():
    print("PySpark Structured Streaming Examples")
    print("=" * 45)
    print("\nBefore running examples:")
    print("1. Generate streaming data: python stream_data_generator.py")
    print("2. Choose an example to run:")
    print("   - Example 1: Basic file streaming")
    print("   - Example 2: Windowed aggregation")
    print("   - Example 3: Multiple output sinks")
    print("   - Example 4: Stateful operations")
    
    try:
        choice = int(input("\nEnter example number (1-4): "))
        print(f"\nRunning Example {choice}...")
        run_example(choice)
        
    except ValueError:
        print("Please enter a valid number (1-4)")
    except KeyboardInterrupt:
        print("\nExample interrupted by user")
    except Exception as e:
        print(f"Error running example: {e}")

if __name__ == "__main__":
    main()