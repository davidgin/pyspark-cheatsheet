from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp, explode, split, sum, to_timestamp, window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
# Most streaming functionality is accessed through DataFrame methods
from schemas import DataSchemas, SchemaUtils
import os

def create_streaming_spark_session() -> SparkSession:
    """Create Spark session optimized for streaming"""
    return SparkSession.builder \
        .appName("PySpark Structured Streaming Practice") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoints") \
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
        .getOrCreate()

def exercise_1_file_stream(spark: SparkSession) -> None:
    """Exercise 1: File-based streaming with explicit schema"""
    print("=== EXERCISE 1: File Stream with Type Safety ===")
    print("This exercise monitors a directory for new CSV files using explicit schemas")
    
    # Use predefined schema instead of inference
    schema = DataSchemas.user_activity_raw_schema()
    
    print("Using explicit schema:")
    SchemaUtils.print_schema_definition(schema, "User Activity Schema")
    
    print("TODO: Complete file streaming exercise:")
    print("1. Read streaming data from 'streaming_data' directory")
    print("2. Parse timestamp and add current processing time")
    print("3. Filter actions and aggregate by user")
    print("4. Write results to console")
    
    # Create streaming DataFrame
    streaming_df = spark \
        .readStream \
        .option("header", "true") \
        .schema(schema) \
        .csv("streaming_data")
    
    # Uncomment to run:
    # processed_df = streaming_df \
    #     .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    #     .withColumn("processing_time", current_timestamp()) \
    #     .filter(col("action").isin("click", "purchase", "view"))
    #
    # query = processed_df.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .trigger(processingTime='10 seconds') \
    #     .start()
    #
    # query.awaitTermination(timeout=30)

def exercise_2_socket_stream(spark):
    """Exercise 2: Socket-based streaming (simulates real-time data)"""
    print("\n=== EXERCISE 2: Socket Stream ===")
    print("This exercise reads from a socket connection")
    
    print("TODO: Complete socket streaming exercise:")
    print("1. Connect to localhost:9999 socket")
    print("2. Split lines into words")
    print("3. Count word occurrences in real-time")
    print("4. Use sliding windows for analysis")
    
    # Uncomment to run (requires socket server):
    # lines = spark \
    #     .readStream \
    #     .format("socket") \
    #     .option("host", "localhost") \
    #     .option("port", 9999) \
    #     .load()
    #
    # words = lines.select(explode(split(lines.value, " ")).alias("word"))
    # word_counts = words.groupBy("word").count()
    #
    # query = word_counts.writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .start()

def exercise_3_windowed_aggregation(spark):
    """Exercise 3: Windowed aggregations with event time"""
    print("\n=== EXERCISE 3: Windowed Aggregations ===")
    print("This exercise demonstrates time-based windows")
    
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True)
    ])
    
    print("TODO: Complete windowed aggregation exercise:")
    print("1. Read streaming data with event time")
    print("2. Create 5-minute tumbling windows")
    print("3. Calculate revenue per window")
    print("4. Handle late data with watermarking")
    
    # Example implementation:
    # streaming_df = spark \
    #     .readStream \
    #     .option("header", "true") \
    #     .schema(schema) \
    #     .csv("streaming_data")
    #
    # windowed_df = streaming_df \
    #     .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")) \
    #     .withWatermark("event_time", "10 minutes") \
    #     .groupBy(
    #         window(col("event_time"), "5 minutes", "5 minutes"),
    #         col("product_id")
    #     ) \
    #     .agg(
    #         sum("amount").alias("total_revenue"),
    #         count("*").alias("transaction_count")
    #     )

def exercise_4_stateful_processing(spark):
    """Exercise 4: Stateful processing and sessionization"""
    print("\n=== EXERCISE 4: Stateful Processing ===")
    print("This exercise tracks user sessions over time")
    
    print("TODO: Complete stateful processing exercise:")
    print("1. Track user sessions based on activity")
    print("2. Update state for each user")
    print("3. Detect session timeouts")
    print("4. Calculate session metrics")
    
    # This requires more complex state management
    # Use mapGroupsWithState for advanced stateful operations

def exercise_5_multiple_sinks(spark):
    """Exercise 5: Multiple output sinks"""
    print("\n=== EXERCISE 5: Multiple Sinks ===")
    print("This exercise writes streaming data to multiple destinations")
    
    print("TODO: Complete multiple sinks exercise:")
    print("1. Read streaming data")
    print("2. Write raw data to Parquet files")
    print("3. Write aggregated data to console")
    print("4. Write alerts to a separate stream")
    
    # Example of multiple outputs:
    # streaming_df = ... # your streaming DataFrame
    #
    # # Sink 1: Raw data to files
    # raw_query = streaming_df.writeStream \
    #     .format("parquet") \
    #     .option("path", "output/raw_data") \
    #     .option("checkpointLocation", "checkpoints/raw") \
    #     .start()
    #
    # # Sink 2: Aggregated data to console
    # agg_query = streaming_df.groupBy("user_id").count().writeStream \
    #     .format("console") \
    #     .option("checkpointLocation", "checkpoints/agg") \
    #     .start()

def exercise_6_stream_stream_joins(spark):
    """Exercise 6: Stream-to-stream joins"""
    print("\n=== EXERCISE 6: Stream-Stream Joins ===")
    print("This exercise joins two streaming DataFrames")
    
    print("TODO: Complete stream-stream joins exercise:")
    print("1. Create two streaming sources")
    print("2. Join streams on common keys")
    print("3. Handle watermarking for both streams")
    print("4. Process joined results")

def create_sample_streaming_data():
    """Helper function to create sample streaming data"""
    print("\n=== Creating Sample Data for Streaming ===")
    
    # Create directories
    os.makedirs("streaming_data", exist_ok=True)
    os.makedirs("checkpoints", exist_ok=True)
    
    sample_data = [
        "timestamp,user_id,action,value",
        "2024-01-20 10:00:00,1,click,1.0",
        "2024-01-20 10:01:00,2,view,2.5",
        "2024-01-20 10:02:00,1,purchase,99.99",
        "2024-01-20 10:03:00,3,click,1.0",
        "2024-01-20 10:04:00,2,purchase,149.99"
    ]
    
    # Write initial sample file
    with open("streaming_data/sample_001.csv", "w") as f:
        f.write("\n".join(sample_data))
    
    print("Sample streaming data created in 'streaming_data' directory")
    print("You can add more CSV files to this directory to simulate streaming")

def run_socket_server_instructions():
    """Instructions for running socket server for testing"""
    print("\n=== Socket Server Setup ===")
    print("To test socket streaming, run this command in another terminal:")
    print("nc -lk 9999")
    print("Then type messages and press Enter to send them to the stream")
    print("On Mac/Linux, you might need: brew install netcat or apt-get install netcat")

def main():
    spark = create_streaming_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Welcome to PySpark Structured Streaming Exercises!")
    print("=" * 60)
    
    try:
        # Create sample data first
        create_sample_streaming_data()
        
        # Show all exercises
        exercise_1_file_stream(spark)
        exercise_2_socket_stream(spark)
        exercise_3_windowed_aggregation(spark)
        exercise_4_stateful_processing(spark)
        exercise_5_multiple_sinks(spark)
        exercise_6_stream_stream_joins(spark)
        
        # Additional setup instructions
        run_socket_server_instructions()
        
        print("\n" + "=" * 60)
        print("Streaming exercises loaded!")
        print("Uncomment code blocks in each exercise to run them.")
        print("Remember: Streaming queries run continuously until stopped.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()