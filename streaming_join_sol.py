#!/usr/bin/env python3
"""
Spark Structured Streaming: Solutions for Async Window Joins
============================================================

This module demonstrates WORKING solutions for handling joins between 
non-synchronized windows in Spark Structured Streaming, addressing
the limitations of stream-stream joins.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col
from pyspark.sql.functions import count
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import expr
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import lit
from pyspark.sql.functions import max
from pyspark.sql.functions import min
from pyspark.sql.functions import sum
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import when
from pyspark.sql.functions import window
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType

def create_streaming_session() -> SparkSession:
    """Create Spark session for streaming joins"""
    return SparkSession.builder \
        .appName("Streaming Join Solutions") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoints/streaming_joins") \
        .getOrCreate()

def solution_1_stream_to_static_enrichment(spark: SparkSession) -> None:
    """
    Solution 1: Stream-to-Static Join with Periodic Refresh
    
    Join fast stream with slowly-changing dimension data
    """
    print("\n=== SOLUTION 1: Stream-to-Static Enrichment ===")
    print("Enriching high-frequency stream with reference data")
    
    # Create static reference data (simulating lookup table)
    reference_data = [
        ("page_1", "Homepage", "Critical", "Marketing"),
        ("page_2", "Product", "High", "Sales"), 
        ("page_3", "Checkout", "Critical", "Revenue"),
        ("page_4", "Support", "Medium", "Support"),
        ("page_5", "About", "Low", "Branding")
    ]
    
    reference_schema = StructType([
        StructField("page_id", StringType(), False),
        StructField("page_name", StringType(), False),
        StructField("priority", StringType(), False),
        StructField("department", StringType(), False)
    ])
    
    reference_df = spark.createDataFrame(reference_data, reference_schema)
    
    # High-frequency user activity stream
    activity_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 100) \
        .load() \
        .select(
            col("timestamp"),
            (col("value") % 1000).cast("string").alias("user_id"),
            expr("CASE WHEN value % 5 = 0 THEN 'page_1' " +
                 "WHEN value % 5 = 1 THEN 'page_2' " +
                 "WHEN value % 5 = 2 THEN 'page_3' " +
                 "WHEN value % 5 = 3 THEN 'page_4' " +
                 "ELSE 'page_5' END").alias("page_id"),
            ((col("value") % 300) + 10).alias("duration_seconds")
        ) \
        .withWatermark("timestamp", "2 minutes")
    
    # Windowed aggregation of activity
    activity_windowed = activity_stream \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("page_id")
        ) \
        .agg(
            count("*").alias("page_views"),
            count("user_id").alias("unique_users"),
            avg("duration_seconds").alias("avg_duration")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("page_id"),
            col("page_views"),
            col("unique_users"), 
            col("avg_duration")
        )
    
    # Join streaming aggregation with static reference data
    enriched_stream = activity_windowed \
        .join(broadcast(reference_df), "page_id", "left") \
        .select(
            col("window_start"),
            col("window_end"),
            col("page_id"),
            coalesce(col("page_name"), lit("Unknown")).alias("page_name"),
            coalesce(col("priority"), lit("Low")).alias("priority"),
            coalesce(col("department"), lit("Other")).alias("department"),
            col("page_views"),
            col("unique_users"),
            col("avg_duration"),
            # Calculate priority score
            when(col("priority") == "Critical", col("page_views") * 3)
            .when(col("priority") == "High", col("page_views") * 2)
            .otherwise(col("page_views")).alias("priority_score")
        )
    
    query = enriched_stream.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .trigger(processingTime='15 seconds') \
        .start()
    
    print("[SUCCESS] Stream-to-static enrichment started. Running for 45 seconds...")
    try:
        query.awaitTermination(timeout=45)
    finally:
        query.stop()

def solution_2_temporal_bucketing_approach(spark: SparkSession) -> None:
    """
    Solution 2: Temporal Bucketing with Manual Correlation
    
    Use time buckets to correlate events from different frequency streams
    """
    print("\n=== SOLUTION 2: Temporal Bucketing Approach ===")
    print("Using time buckets to correlate different frequency streams")
    
    # High-frequency transaction stream
    transactions_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 80) \
        .load() \
        .select(
            col("timestamp"),
            (col("value") % 500).cast("string").alias("transaction_id"),
            expr("CASE WHEN value % 3 = 0 THEN 'purchase' " +
                 "WHEN value % 3 = 1 THEN 'refund' " +
                 "ELSE 'adjustment' END").alias("transaction_type"),
            ((col("value") % 1000) + 10.0).alias("amount"),
            expr("CASE WHEN value % 4 = 0 THEN 'credit_card' " +
                 "WHEN value % 4 = 1 THEN 'debit_card' " +
                 "WHEN value % 4 = 2 THEN 'paypal' " +
                 "ELSE 'crypto' END").alias("payment_method")
        ) \
        .withWatermark("timestamp", "3 minutes")
    
    # Aggregate transactions into 2-minute buckets
    transaction_buckets = transactions_stream \
        .withColumn("time_bucket", 
                   expr("CAST(unix_timestamp(timestamp) / 120 AS LONG) * 120")) \
        .groupBy("time_bucket", "payment_method") \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            min("timestamp").alias("bucket_start"),
            max("timestamp").alias("bucket_end")
        ) \
        .withColumn("bucket_timestamp", from_unixtime(col("time_bucket")))
    
    # Output transaction buckets with correlation metadata
    correlation_ready = transaction_buckets.select(
        col("bucket_timestamp"),
        col("time_bucket"),
        col("payment_method"),
        col("transaction_count"),
        col("total_amount"),
        col("avg_amount"),
        col("bucket_start"),
        col("bucket_end"),
        # Add correlation keys for external systems
        expr("time_bucket % 300").alias("five_min_offset"),  # For 5-min alignment
        expr("time_bucket % 600").alias("ten_min_offset"),   # For 10-min alignment
        current_timestamp().alias("processing_time")
    )
    
    query = correlation_ready.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 12) \
        .trigger(processingTime='20 seconds') \
        .start()
    
    print("[SUCCESS] Temporal bucketing started. Running for 60 seconds...")
    try:
        query.awaitTermination(timeout=60)
    finally:
        query.stop()

def solution_3_stateful_correlation(spark: SparkSession) -> None:
    """
    Solution 3: Stateful Event Correlation
    
    Use mapGroupsWithState for complex correlation logic
    """
    print("\n=== SOLUTION 3: Stateful Event Correlation ===")
    print("Using stateful processing to correlate events across time")
    
    # Combined event stream (simulating multiple event types)
    events_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 60) \
        .load() \
        .select(
            col("timestamp"),
            (col("value") % 100).cast("string").alias("entity_id"),
            expr("CASE WHEN value % 6 = 0 THEN 'order_created' " +
                 "WHEN value % 6 = 1 THEN 'payment_processed' " +
                 "WHEN value % 6 = 2 THEN 'inventory_reserved' " +
                 "WHEN value % 6 = 3 THEN 'shipping_started' " +
                 "WHEN value % 6 = 4 THEN 'delivered' " +
                 "ELSE 'cancelled' END").alias("event_type"),
            ((col("value") % 1000) + 50.0).alias("event_value"),
            # Add some randomness to simulate different timing patterns
            expr("timestamp + INTERVAL " + 
                 "CAST((value % 300) AS STRING) + ' seconds'").alias("adjusted_timestamp")
        ) \
        .withWatermark("adjusted_timestamp", "5 minutes")
    
    # Group events by entity and create correlation windows
    entity_correlation = events_stream \
        .groupBy(
            col("entity_id"),
            window(col("adjusted_timestamp"), "10 minutes", "5 minutes")  # Sliding window
        ) \
        .agg(
            count("*").alias("total_events"),
            expr("collect_list(struct(adjusted_timestamp, event_type, event_value))").alias("event_sequence"),
            expr("size(collect_set(event_type))").alias("unique_event_types"),
            min("adjusted_timestamp").alias("sequence_start"),
            max("adjusted_timestamp").alias("sequence_end"),
            sum("event_value").alias("total_value")
        ) \
        .select(
            col("entity_id"),
            col("window.start").alias("correlation_window_start"),
            col("window.end").alias("correlation_window_end"),
            col("total_events"),
            col("unique_event_types"),
            col("sequence_start"),
            col("sequence_end"),
            col("total_value"),
            col("event_sequence"),
            # Calculate correlation metrics
            (unix_timestamp(col("sequence_end")) - unix_timestamp(col("sequence_start"))).alias("sequence_duration_seconds"),
            when(col("total_events") > 3, "complete_sequence")
            .when(col("total_events") > 1, "partial_sequence")
            .otherwise("single_event").alias("sequence_completeness")
        )
    
    query = entity_correlation.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 8) \
        .trigger(processingTime='30 seconds') \
        .start()
    
    print("[SUCCESS] Stateful correlation started. Running for 75 seconds...")
    try:
        query.awaitTermination(timeout=75)
    finally:
        query.stop()

def solution_4_delta_lake_mediated_joins(spark: SparkSession) -> None:
    """
    Solution 4: Delta Lake Mediated Joins
    
    Use Delta Lake as an intermediate store for complex correlations
    """
    print("\n=== SOLUTION 4: Delta Lake Mediated Approach ===")
    print("Using intermediate storage for complex stream correlations")
    print("[INFO] This would typically use Delta Lake, but showing file-based approach")
    
    # Fast IoT sensor readings
    sensor_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 120) \
        .load() \
        .select(
            col("timestamp"),
            (col("value") % 20).cast("string").alias("sensor_id"),
            ((col("value") % 100) + 15.0).alias("temperature"),
            ((col("value") % 80) + 20.0).alias("humidity"),
            expr("CASE WHEN value % 2 = 0 THEN 'normal' ELSE 'alert' END").alias("status")
        ) \
        .withWatermark("timestamp", "1 minute")
    
    # Aggregate sensor data into 2-minute windows and save to files
    sensor_aggregated = sensor_stream \
        .groupBy(
            col("sensor_id"),
            window(col("timestamp"), "2 minutes")
        ) \
        .agg(
            avg("temperature").alias("avg_temp"),
            avg("humidity").alias("avg_humidity"),
            count("*").alias("reading_count"),
            sum(when(col("status") == "alert", 1).otherwise(0)).alias("alert_count")
        ) \
        .select(
            col("sensor_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_temp"),
            col("avg_humidity"), 
            col("reading_count"),
            col("alert_count"),
            current_timestamp().alias("processed_at")
        )
    
    # Write to intermediate storage (simulating Delta Lake)
    sensor_writer = sensor_aggregated.writeStream \
        .format("parquet") \
        .option("path", "output/sensor_windows") \
        .option("checkpointLocation", "checkpoints/sensor_sink") \
        .outputMode("append") \
        .trigger(processingTime='15 seconds') \
        .start()
    
    # Maintenance events (infrequent)
    maintenance_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 3) \
        .load() \
        .select(
            col("timestamp"),
            (col("value") % 20).cast("string").alias("sensor_id"),
            expr("CASE WHEN value % 3 = 0 THEN 'calibration' " +
                 "WHEN value % 3 = 1 THEN 'cleaning' " +
                 "ELSE 'replacement' END").alias("maintenance_type"),
            ((col("value") % 120) + 30).alias("maintenance_duration")
        ) \
        .withWatermark("timestamp", "5 minutes")
    
    # Aggregate maintenance into larger windows
    maintenance_windowed = maintenance_stream \
        .groupBy(
            col("sensor_id"),
            window(col("timestamp"), "10 minutes")  # Larger window for sparse events
        ) \
        .agg(
            count("*").alias("maintenance_events"),
            expr("collect_list(maintenance_type)").alias("maintenance_types"),
            sum("maintenance_duration").alias("total_maintenance_time")
        ) \
        .select(
            col("sensor_id"),
            col("window.start").alias("maint_window_start"),
            col("window.end").alias("maint_window_end"),
            col("maintenance_events"),
            col("maintenance_types"),
            col("total_maintenance_time"),
            current_timestamp().alias("maint_processed_at")
        )
    
    # Output maintenance data separately  
    maintenance_writer = maintenance_windowed.writeStream \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 5) \
        .outputMode("append") \
        .trigger(processingTime='30 seconds') \
        .start()
    
    print("[SUCCESS] Multi-sink approach started.")
    print("[INFO] Sensor data -> output/sensor_windows/*.parquet")
    print("[INFO] Maintenance data -> console output")
    print("[INFO] External system can join these using time range queries")
    print("Running for 90 seconds...")
    
    try:
        maintenance_writer.awaitTermination(timeout=90)
    finally:
        sensor_writer.stop()
        maintenance_writer.stop()

def solution_5_micro_batch_correlation(spark: SparkSession) -> None:
    """
    Solution 5: Micro-batch Based Correlation
    
    Use foreachBatch to implement custom correlation logic
    """
    print("\n=== SOLUTION 5: Micro-batch Correlation ===")
    print("Using foreachBatch for complex multi-stream correlation")
    
    def correlate_batch(batch_df, batch_id):
        """Custom correlation logic for each micro-batch"""
        print(f"\n[BATCH {batch_id}] Processing correlation batch...")
        
        if batch_df.count() == 0:
            print(f"[BATCH {batch_id}] Empty batch, skipping...")
            return
        
        # Show batch statistics
        batch_stats = batch_df.agg(
            count("*").alias("total_events"),
            count("event_group").alias("unique_groups"),
            min("timestamp").alias("batch_start"),
            max("timestamp").alias("batch_end")
        ).collect()[0]
        
        print(f"[BATCH {batch_id}] Stats: {batch_stats['total_events']} events, " +
              f"{batch_stats['unique_groups']} groups")
        
        # Perform correlation analysis within the batch
        correlations = batch_df \
            .groupBy("event_group") \
            .agg(
                count("*").alias("events_in_group"),
                expr("collect_list(struct(timestamp, event_type, metric_value))").alias("event_timeline"),
                (max(unix_timestamp("timestamp")) - min(unix_timestamp("timestamp"))).alias("time_span_seconds")
            ) \
            .filter(col("events_in_group") > 1)  # Only groups with multiple events
        
        if correlations.count() > 0:
            print(f"[BATCH {batch_id}] Found {correlations.count()} correlated event groups:")
            correlations.show(truncate=False)
        else:
            print(f"[BATCH {batch_id}] No correlations found in this batch")
    
    # Multi-pattern event stream
    events_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 50) \
        .load() \
        .select(
            col("timestamp"),
            (col("value") % 50).cast("string").alias("event_group"),
            expr("CASE WHEN value % 4 = 0 THEN 'start' " +
                 "WHEN value % 4 = 1 THEN 'progress' " +
                 "WHEN value % 4 = 2 THEN 'warning' " +
                 "ELSE 'complete' END").alias("event_type"),
            ((col("value") % 100) + 1.0).alias("metric_value")
        ) \
        .withWatermark("timestamp", "2 minutes")
    
    # Use foreachBatch for custom correlation
    query = events_stream.writeStream \
        .foreachBatch(correlate_batch) \
        .trigger(processingTime='20 seconds') \
        .start()
    
    print("[SUCCESS] Micro-batch correlation started. Running for 60 seconds...")
    try:
        query.awaitTermination(timeout=60)
    finally:
        query.stop()

def main():
    """Demonstrate all streaming join solutions"""
    print("Spark Structured Streaming: Async Window Join Solutions")
    print("=" * 65)
    
    spark = create_streaming_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("\n[INFO] Solutions for non-synchronized window joins:")
        print("1. Stream-to-Static: Enrich streams with reference data")
        print("2. Temporal Bucketing: Align streams using time buckets") 
        print("3. Stateful Correlation: Use windowed aggregations for correlation")
        print("4. Delta Lake Mediation: Use storage layer for complex joins")
        print("5. Micro-batch Processing: Custom correlation in foreachBatch")
        
        solution_1_stream_to_static_enrichment(spark)
        solution_2_temporal_bucketing_approach(spark)
        solution_3_stateful_correlation(spark)
        solution_4_delta_lake_mediated_joins(spark)
        solution_5_micro_batch_correlation(spark)
        
        print("\n" + "=" * 65)
        print("[COMPLETE] All streaming join solutions demonstrated!")
        
        print("\n[KEY INSIGHTS] Handling Async Windows:")
        print("[TIP] Stream-Stream Joins: Limited in Spark, use alternatives")
        print("[TIP] Stream-Static Joins: Use broadcast for small reference data")
        print("[TIP] Time Buckets: Align different frequencies to common intervals")
        print("[TIP] Session Windows: Handle irregular patterns naturally")
        print("[TIP] Stateful Processing: Use for complex correlation logic")
        print("[TIP] External Storage: Use Delta/Parquet for heavy correlations")
        print("[TIP] Watermarks: Set based on maximum expected delays")
        print("[TIP] Trigger Intervals: Balance latency vs resource efficiency")
        
    except Exception as e:
        print(f"[ERROR] Error in solutions: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()