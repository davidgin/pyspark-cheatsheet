#!/usr/bin/env python3
"""
Spark Structured Streaming: Handling Joins Between Non-Synchronized Windows
============================================================================

This module demonstrates advanced techniques for joining streaming data with:
1. Different window sizes and frequencies
2. Misaligned time boundaries  
3. Varying event arrival patterns
4. Late-arriving data handling
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct
from pyspark.sql.functions import avg
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col
from pyspark.sql.functions import count
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import expr
from pyspark.sql.functions import first
from pyspark.sql.functions import last
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
import time
from datetime import datetime, timedelta

def create_streaming_session() -> SparkSession:
    """Create optimized Spark session for streaming joins"""
    return SparkSession.builder \
        .appName("Async Window Joins") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoints/async_joins") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def strategy_1_overlapping_windows(spark: SparkSession) -> None:
    """
    Strategy 1: Overlapping Windows with Different Sizes
    
    Use case: Join high-frequency metrics (1-min windows) with 
    low-frequency aggregates (5-min windows)
    """
    print("\n=== STRATEGY 1: Overlapping Windows ===")
    print("Joining streams with different window sizes using overlap")
    
    # High-frequency stream schema (user activity)
    activity_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("page_id", StringType(), True),
        StructField("duration", IntegerType(), True)
    ])
    
    # Low-frequency stream schema (system metrics)  
    metrics_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("cpu_usage", DoubleType(), True),
        StructField("memory_usage", DoubleType(), True),
        StructField("region", StringType(), True)
    ])
    
    # Create streaming DataFrames (simulate with rate sources)
    print("Creating high-frequency activity stream (1-minute windows)...")
    activity_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 50) \
        .load() \
        .select(
            col("timestamp").cast("string").alias("timestamp"),
            (col("value") % 1000).cast("string").alias("user_id"),
            expr("CASE WHEN value % 4 = 0 THEN 'click' " +
                 "WHEN value % 4 = 1 THEN 'view' " +
                 "WHEN value % 4 = 2 THEN 'scroll' " +
                 "ELSE 'exit' END").alias("action"),
            (col("value") % 100).cast("string").alias("page_id"),
            ((col("value") % 300) + 10).alias("duration")
        ) \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withWatermark("timestamp", "2 minutes")
    
    print("Creating low-frequency metrics stream (5-minute windows)...")  
    metrics_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 10) \
        .load() \
        .select(
            col("timestamp").cast("string").alias("timestamp"),
            (col("value") % 10).cast("string").alias("server_id"),
            ((col("value") % 100) + 20.0).alias("cpu_usage"),
            ((col("value") % 80) + 40.0).alias("memory_usage"),
            expr("CASE WHEN value % 3 = 0 THEN 'us-east' " +
                 "WHEN value % 3 = 1 THEN 'us-west' " +
                 "ELSE 'eu-west' END").alias("region")
        ) \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withWatermark("timestamp", "5 minutes")
    
    # Aggregate with different window sizes
    activity_windows = activity_stream \
        .groupBy(
            window(col("timestamp"), "1 minute", "1 minute"),
            col("page_id")
        ) \
        .agg(
            count("*").alias("activity_count"),
            approx_count_distinct("user_id").alias("unique_users"),
            avg("duration").alias("avg_duration")
        ) \
        .select(
            col("window.start").alias("activity_window_start"),
            col("window.end").alias("activity_window_end"), 
            col("page_id"),
            col("activity_count"),
            col("unique_users"),
            col("avg_duration")
        )
    
    metrics_windows = metrics_stream \
        .groupBy(
            window(col("timestamp"), "5 minutes", "5 minutes"),
            col("region")
        ) \
        .agg(
            avg("cpu_usage").alias("avg_cpu"),
            avg("memory_usage").alias("avg_memory"),
            count("*").alias("metric_count")
        ) \
        .select(
            col("window.start").alias("metrics_window_start"),
            col("window.end").alias("metrics_window_end"),
            col("region"), 
            col("avg_cpu"),
            col("avg_memory"),
            col("metric_count")
        )
    
    # Join using overlapping time ranges
    # Activity (1-min) overlaps with Metrics (5-min) if activity window falls within metrics window
    joined_stream = activity_windows.join(
        metrics_windows,
        (col("activity_window_start") >= col("metrics_window_start")) & 
        (col("activity_window_start") < col("metrics_window_end")),
        "leftOuter"  # Left outer to keep all activity windows
    ).select(
        col("activity_window_start"),
        col("activity_window_end"),
        col("page_id"),
        col("activity_count"), 
        col("unique_users"),
        col("avg_duration"),
        col("region"),
        coalesce(col("avg_cpu"), lit(0.0)).alias("avg_cpu"),
        coalesce(col("avg_memory"), lit(0.0)).alias("avg_memory"),
        coalesce(col("metric_count"), lit(0)).alias("metric_count")
    )
    
    # Output results
    query = joined_stream.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .trigger(processingTime='30 seconds') \
        .start()
    
    print("[SUCCESS] Overlapping window join started. Running for 60 seconds...")
    try:
        query.awaitTermination(timeout=60)
    finally:
        query.stop()

def strategy_2_temporal_tolerance_joins(spark: SparkSession) -> None:
    """
    Strategy 2: Temporal Tolerance Joins
    
    Use case: Join events that should correlate but may arrive 
    at slightly different times due to network delays
    """
    print("\n=== STRATEGY 2: Temporal Tolerance Joins ===")
    print("Joining events with temporal tolerance for timing variations")
    
    # Order events stream
    orders_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 20) \
        .load() \
        .select(
            col("timestamp").cast("string").alias("event_time"),
            (col("value") % 1000).cast("string").alias("order_id"),
            expr("CASE WHEN value % 3 = 0 THEN 'created' " +
                 "WHEN value % 3 = 1 THEN 'paid' " +
                 "ELSE 'shipped' END").alias("status"),
            ((col("value") % 500) + 50.0).alias("amount")
        ) \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .withWatermark("event_time", "3 minutes")
    
    # Payment events stream (may arrive with delay)
    payments_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 15) \
        .load() \
        .select(
            # Simulate 30-120 second delay in payment events
            (col("timestamp").cast("long") + (col("value") % 90) + 30).cast("timestamp").alias("payment_time"),
            (col("value") % 1000).cast("string").alias("order_id"),
            expr("CASE WHEN value % 2 = 0 THEN 'credit_card' " +
                 "ELSE 'paypal' END").alias("payment_method"),
            ((col("value") % 500) + 50.0).alias("payment_amount")
        ) \
        .withWatermark("payment_time", "5 minutes")
    
    # Create temporal buckets for tolerance-based joins
    orders_bucketed = orders_stream \
        .withColumn("time_bucket", 
                   (unix_timestamp(col("event_time")) / 120).cast("long")) \
        .withColumn("tolerance_start", 
                   col("event_time")) \
        .withColumn("tolerance_end", 
                   expr("event_time + INTERVAL 2 MINUTES"))
    
    payments_bucketed = payments_stream \
        .withColumn("time_bucket", 
                   (unix_timestamp(col("payment_time")) / 120).cast("long")) \
        .withColumn("payment_tolerance_start", 
                   expr("payment_time - INTERVAL 2 MINUTES")) \
        .withColumn("payment_tolerance_end", 
                   col("payment_time"))
    
    # Join with temporal tolerance
    # Match orders with payments that arrive within tolerance window
    tolerance_joined = orders_bucketed.join(
        payments_bucketed,
        (col("order_id") == col("order_id")) &
        (col("payment_time") >= col("tolerance_start")) &
        (col("payment_time") <= col("tolerance_end")),
        "leftOuter"
    ).select(
        col("event_time").alias("order_time"),
        col("order_id"),
        col("status"),
        col("amount").alias("order_amount"),
        col("payment_time"),
        col("payment_method"),
        col("payment_amount"),
        when(col("payment_time").isNotNull(), 
             unix_timestamp(col("payment_time")) - unix_timestamp(col("event_time")))
        .otherwise(None).alias("payment_delay_seconds")
    )
    
    query = tolerance_joined.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 8) \
        .trigger(processingTime='20 seconds') \
        .start()
    
    print("[SUCCESS] Temporal tolerance join started. Running for 60 seconds...")
    try:
        query.awaitTermination(timeout=60)
    finally:
        query.stop()

def strategy_3_session_based_joins(spark: SparkSession) -> None:
    """
    Strategy 3: Session-Based Joins with Gap Detection
    
    Use case: Join user sessions where events have irregular timing
    but belong to logical sessions with natural boundaries
    """
    print("\n=== STRATEGY 3: Session-Based Joins ===")
    print("Using session windows to join irregular event streams")
    
    # User interaction events
    interactions_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 25) \
        .load() \
        .select(
            col("timestamp").cast("string").alias("interaction_time"),
            (col("value") % 200).cast("string").alias("user_id"),
            expr("CASE WHEN value % 5 = 0 THEN 'login' " +
                 "WHEN value % 5 = 1 THEN 'search' " +
                 "WHEN value % 5 = 2 THEN 'click' " +
                 "WHEN value % 5 = 3 THEN 'purchase' " +
                 "ELSE 'logout' END").alias("interaction_type"),
            (col("value") % 1000).cast("string").alias("session_context")
        ) \
        .withColumn("interaction_time", to_timestamp(col("interaction_time"))) \
        .withWatermark("interaction_time", "10 minutes")
    
    # System events (may be sparse)
    system_events_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 8) \
        .load() \
        .select(
            col("timestamp").cast("string").alias("system_time"),
            (col("value") % 200).cast("string").alias("user_id"),
            expr("CASE WHEN value % 3 = 0 THEN 'performance_alert' " +
                 "WHEN value % 3 = 1 THEN 'security_event' " +
                 "ELSE 'system_error' END").alias("event_type"),
            ((col("value") % 5) + 1).alias("severity")
        ) \
        .withColumn("system_time", to_timestamp(col("system_time"))) \
        .withWatermark("system_time", "10 minutes")
    
    # Create session windows with 5-minute gap timeout
    from pyspark.sql.functions import session_window
    
    interaction_sessions = interactions_stream \
        .groupBy(
            col("user_id"),
            session_window(col("interaction_time"), "5 minutes")
        ) \
        .agg(
            count("*").alias("interaction_count"),
            first("interaction_time").alias("session_start"),
            last("interaction_time").alias("session_end"),
            expr("collect_list(interaction_type)").alias("interactions"),
            approx_count_distinct("session_context").alias("unique_contexts")
        ) \
        .select(
            col("user_id"),
            col("session_window.start").alias("session_window_start"),
            col("session_window.end").alias("session_window_end"),
            col("session_start"),
            col("session_end"),
            col("interaction_count"),
            col("interactions"),
            col("unique_contexts")
        )
    
    system_sessions = system_events_stream \
        .groupBy(
            col("user_id"),
            session_window(col("system_time"), "10 minutes")  # Longer gap for sparse events
        ) \
        .agg(
            count("*").alias("system_event_count"),
            first("system_time").alias("first_system_event"),
            last("system_time").alias("last_system_event"),
            expr("collect_list(event_type)").alias("system_events"),
            avg("severity").alias("avg_severity")
        ) \
        .select(
            col("user_id"),
            col("session_window.start").alias("system_session_start"),
            col("session_window.end").alias("system_session_end"),
            col("first_system_event"),
            col("last_system_event"),
            col("system_event_count"),
            col("system_events"),
            col("avg_severity")
        )
    
    # Join sessions with overlap detection
    session_joined = interaction_sessions.join(
        system_sessions,
        (col("user_id") == col("user_id")) &
        # Sessions overlap if interaction session overlaps with system session
        (col("session_start") <= col("system_session_end")) &
        (col("session_end") >= col("system_session_start")),
        "leftOuter"
    ).select(
        col("user_id"),
        col("session_window_start"),
        col("session_window_end"),
        col("interaction_count"),
        col("interactions"),
        col("unique_contexts"),
        coalesce(col("system_event_count"), lit(0)).alias("system_event_count"),
        col("system_events"),
        col("avg_severity"),
        when(col("first_system_event").isNotNull(),
             unix_timestamp(col("first_system_event")) - unix_timestamp(col("session_start")))
        .otherwise(None).alias("system_event_offset_seconds")
    )
    
    query = session_joined.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 5) \
        .trigger(processingTime='45 seconds') \
        .start()
    
    print("[SUCCESS] Session-based join started. Running for 75 seconds...")
    try:
        query.awaitTermination(timeout=75)
    finally:
        query.stop()

def strategy_4_multi_stream_correlation(spark: SparkSession) -> None:
    """
    Strategy 4: Multi-Stream Correlation with Event-Time Alignment
    
    Use case: Correlate multiple streams with different characteristics
    using event-time alignment and flexible matching
    """
    print("\n=== STRATEGY 4: Multi-Stream Correlation ===")
    print("Correlating multiple streams with event-time alignment")
    
    # IoT sensor data (frequent, regular)
    sensor_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 40) \
        .load() \
        .select(
            col("timestamp").cast("string").alias("sensor_timestamp"),
            (col("value") % 50).cast("string").alias("device_id"),
            ((col("value") % 100) + 15.0).alias("temperature"),
            ((col("value") % 80) + 30.0).alias("humidity"),
            expr("CASE WHEN value % 4 = 0 THEN 'indoor' ELSE 'outdoor' END").alias("location_type")
        ) \
        .withColumn("sensor_timestamp", to_timestamp(col("sensor_timestamp"))) \
        .withWatermark("sensor_timestamp", "2 minutes")
    
    # Alert stream (infrequent, irregular)
    alert_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 5) \
        .load() \
        .select(
            col("timestamp").cast("string").alias("alert_timestamp"),
            (col("value") % 50).cast("string").alias("device_id"),
            expr("CASE WHEN value % 3 = 0 THEN 'high_temp' " +
                 "WHEN value % 3 = 1 THEN 'low_humidity' " +
                 "ELSE 'sensor_offline' END").alias("alert_type"),
            ((col("value") % 3) + 1).alias("priority")
        ) \
        .withColumn("alert_timestamp", to_timestamp(col("alert_timestamp"))) \
        .withWatermark("alert_timestamp", "5 minutes")
    
    # Maintenance log stream (very infrequent)
    maintenance_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 2) \
        .load() \
        .select(
            col("timestamp").cast("string").alias("maintenance_timestamp"),
            (col("value") % 50).cast("string").alias("device_id"),
            expr("CASE WHEN value % 2 = 0 THEN 'scheduled' ELSE 'emergency' END").alias("maintenance_type"),
            ((col("value") % 60) + 30).alias("duration_minutes")
        ) \
        .withColumn("maintenance_timestamp", to_timestamp(col("maintenance_timestamp"))) \
        .withWatermark("maintenance_timestamp", "10 minutes")
    
    # Create unified time-based buckets (5-minute buckets)
    sensor_bucketed = sensor_stream \
        .withColumn("time_bucket", 
                   expr("CAST(unix_timestamp(sensor_timestamp) / 300 AS LONG) * 300")) \
        .groupBy("device_id", "time_bucket", "location_type") \
        .agg(
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity"),
            count("*").alias("sensor_readings"),
            min("sensor_timestamp").alias("period_start"),
            max("sensor_timestamp").alias("period_end")
        )
    
    alert_bucketed = alert_stream \
        .withColumn("time_bucket", 
                   expr("CAST(unix_timestamp(alert_timestamp) / 300 AS LONG) * 300")) \
        .groupBy("device_id", "time_bucket") \
        .agg(
            count("*").alias("alert_count"),
            expr("collect_list(alert_type)").alias("alert_types"),
            max("priority").alias("max_priority"),
            first("alert_timestamp").alias("first_alert")
        )
    
    maintenance_bucketed = maintenance_stream \
        .withColumn("time_bucket", 
                   expr("CAST(unix_timestamp(maintenance_timestamp) / 300 AS LONG) * 300")) \
        .groupBy("device_id", "time_bucket") \
        .agg(
            count("*").alias("maintenance_count"),
            expr("collect_list(maintenance_type)").alias("maintenance_types"),
            sum("duration_minutes").alias("total_maintenance_minutes"),
            first("maintenance_timestamp").alias("maintenance_start")
        )
    
    # Multi-way join on device_id and time_bucket
    correlated_stream = sensor_bucketed \
        .join(alert_bucketed, ["device_id", "time_bucket"], "leftOuter") \
        .join(maintenance_bucketed, ["device_id", "time_bucket"], "leftOuter") \
        .select(
            col("device_id"),
            from_unixtime(col("time_bucket")).alias("time_period"),
            col("location_type"),
            col("avg_temperature"),
            col("avg_humidity"),
            col("sensor_readings"),
            coalesce(col("alert_count"), lit(0)).alias("alert_count"),
            col("alert_types"),
            coalesce(col("max_priority"), lit(0)).alias("max_priority"),
            coalesce(col("maintenance_count"), lit(0)).alias("maintenance_count"),
            col("maintenance_types"),
            coalesce(col("total_maintenance_minutes"), lit(0)).alias("total_maintenance_minutes"),
            # Create correlation indicators
            when(col("alert_count") > 0, True).otherwise(False).alias("has_alerts"),
            when(col("maintenance_count") > 0, True).otherwise(False).alias("has_maintenance"),
            when((col("avg_temperature") > 80) | (col("avg_humidity") < 40), True)
            .otherwise(False).alias("environmental_issues")
        )
    
    # Add from_unixtime import
    from pyspark.sql.functions import from_unixtime
    
    query = correlated_stream.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 8) \
        .trigger(processingTime='30 seconds') \
        .start()
    
    print("[SUCCESS] Multi-stream correlation started. Running for 90 seconds...")
    try:
        query.awaitTermination(timeout=90)
    finally:
        query.stop()

def main():
    """Demonstrate all async window join strategies"""
    print("Spark Structured Streaming: Async Window Joins")
    print("=" * 60)
    
    spark = create_streaming_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("\n[INFO] Demonstrating 4 strategies for handling misaligned windows:")
        print("1. Overlapping Windows - Different window sizes with overlap detection")
        print("2. Temporal Tolerance - Join with time tolerance for delayed events") 
        print("3. Session-Based - Use session windows for irregular event patterns")
        print("4. Multi-Stream Correlation - Time-bucket alignment across multiple streams")
        
        # Run each strategy
        strategy_1_overlapping_windows(spark)
        strategy_2_temporal_tolerance_joins(spark)  
        strategy_3_session_based_joins(spark)
        strategy_4_multi_stream_correlation(spark)
        
        print("\n" + "=" * 60)
        print("[COMPLETE] All async window join strategies demonstrated!")
        
        print("\n[SUMMARY] Key Techniques for Non-Synchronized Windows:")
        print("[TIP] Overlapping Windows: Use range conditions for different window sizes")
        print("[TIP] Temporal Tolerance: Add buffer time for delayed events")
        print("[TIP] Session Windows: Group irregular events by natural session boundaries")
        print("[TIP] Time Bucketing: Align multiple streams using common time buckets")
        print("[TIP] Watermarks: Set appropriate watermarks based on expected delays")
        print("[TIP] Outer Joins: Use left/right outer joins to handle missing correlations")
        print("[TIP] Event-Time Processing: Always use event time for accurate correlations")
        
    except Exception as e:
        print(f"[ERROR] Error in demonstrations: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()