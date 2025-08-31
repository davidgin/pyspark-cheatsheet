# Spark Structured Streaming: Async Window Joins Guide

## Problem: Non-Synchronized Windows & Variable Event Frequencies

When dealing with multiple streaming sources, you often face:

1. **Different Window Sizes**: One stream uses 1-minute windows, another uses 5-minute windows
2. **Misaligned Boundaries**: Windows start at different times (e.g., :00, :15, :30 vs :02, :07, :12)
3. **Variable Event Frequencies**: High-frequency vs low-frequency streams
4. **Late-Arriving Data**: Events arrive out of order with different delays
5. **Irregular Patterns**: Some streams have bursty behavior, others are steady

## Core Strategies

### 1. Overlapping Windows Strategy

**When to use**: Different window sizes but overlapping time ranges

```python
# High-frequency stream (1-minute windows)
activity_windows = stream1.groupBy(
    window(col("timestamp"), "1 minute")
).agg(...)

# Low-frequency stream (5-minute windows)  
metrics_windows = stream2.groupBy(
    window(col("timestamp"), "5 minutes")
).agg(...)

# Join where 1-min window falls within 5-min window
joined = activity_windows.join(
    metrics_windows,
    (col("activity_window_start") >= col("metrics_window_start")) & 
    (col("activity_window_start") < col("metrics_window_end"))
)
```

**Key Points**:
- Use range conditions instead of exact timestamp matches
- Left outer joins to preserve all records from primary stream
- Consider window slide intervals for better coverage

### 2. Temporal Tolerance Strategy

**When to use**: Events should correlate but may arrive with timing variations

```python
# Add tolerance buffers
orders_with_tolerance = orders.withColumn(
    "tolerance_end", 
    expr("timestamp + INTERVAL 2 MINUTES")
)

# Join with temporal buffer
tolerance_join = orders_with_tolerance.join(
    payments,
    (col("order_id") == col("order_id")) &
    (col("payment_time") >= col("order_timestamp")) &
    (col("payment_time") <= col("tolerance_end"))
)
```

**Key Points**:
- Define acceptable time drift (e.g., +/- 2 minutes)
- Use watermarks to handle late data
- Monitor delay metrics to tune tolerance windows

### 3. Session-Based Windows Strategy

**When to use**: Irregular event patterns with natural session boundaries

```python
from pyspark.sql.functions import session_window

# Session windows with gap timeout
sessions = stream.groupBy(
    col("user_id"),
    session_window(col("timestamp"), "5 minutes")  # 5-min gap ends session
).agg(...)
```

**Key Points**:
- Session windows adapt to actual event patterns
- Gap timeout defines session boundaries
- Different gap timeouts for different event types
- Join sessions based on overlap rather than exact alignment

### 4. Time Bucketing Strategy

**When to use**: Multiple streams with very different characteristics

```python
# Align streams to common time buckets (5-minute buckets)
stream1_bucketed = stream1.withColumn(
    "time_bucket", 
    expr("CAST(unix_timestamp(timestamp) / 300 AS LONG) * 300")
).groupBy("key", "time_bucket").agg(...)

stream2_bucketed = stream2.withColumn(
    "time_bucket", 
    expr("CAST(unix_timestamp(timestamp) / 300 AS LONG) * 300") 
).groupBy("key", "time_bucket").agg(...)

# Join on common time bucket
joined = stream1_bucketed.join(stream2_bucketed, ["key", "time_bucket"])
```

**Key Points**:
- Choose bucket size based on business requirements
- Larger buckets = more correlation but less precision
- Smaller buckets = more precision but potential missed correlations

## Advanced Techniques

### Sliding Window Overlap

```python
# Create overlapping sliding windows
sliding_windows = stream.groupBy(
    window(col("timestamp"), "10 minutes", "5 minutes")  # 10-min window, 5-min slide
).agg(...)
```

### Multi-Level Aggregation

```python
# First aggregate at fine granularity, then coarser
fine_agg = stream.groupBy(
    window(col("timestamp"), "1 minute")
).agg(...)

# Then aggregate the aggregates
coarse_agg = fine_agg.groupBy(
    window(col("window.start"), "5 minutes")
).agg(...)
```

### Watermark Coordination

```python
# Set different watermarks based on stream characteristics
fast_stream.withWatermark("timestamp", "1 minute")  # Low latency tolerance
slow_stream.withWatermark("timestamp", "10 minutes")  # Higher latency tolerance
```

## Best Practices

### 1. Watermark Management
- Set watermarks based on expected event delays
- Use longer watermarks for streams with higher latency
- Monitor late data metrics to tune watermarks

### 2. Join Strategy Selection
```python
# Use appropriate join types
.join(other_stream, condition, "leftOuter")  # Preserve primary stream
.join(other_stream, condition, "inner")      # Only matching records
```

### 3. Memory Management
- Use `coalesce()` after joins to optimize partition count
- Set appropriate trigger intervals to balance latency vs throughput
- Monitor streaming query metrics

### 4. Late Data Handling
```python
# Handle late arrivals explicitly
result = stream.select(
    col("*"),
    when(col("processing_time") - col("event_time") > expr("INTERVAL 5 MINUTES"), 
         "LATE").otherwise("ON_TIME").alias("arrival_status")
)
```

### 5. State Management
- Use `outputMode("append")` for better performance when possible
- Consider stateful operations impact on memory
- Use checkpointing for fault tolerance

## Performance Considerations

1. **Partition Strategy**: Ensure good partitioning on join keys
2. **Trigger Frequency**: Balance between latency and resource usage
3. **State Size**: Monitor state store size for stateful operations
4. **Watermark Impact**: Aggressive watermarks reduce state but may drop data

## Monitoring & Debugging

```python
# Add monitoring columns
monitored_stream = joined_stream.select(
    col("*"),
    current_timestamp().alias("processing_time"),
    unix_timestamp(current_timestamp()) - unix_timestamp(col("event_time")).alias("processing_delay")
)

# Output to multiple sinks
query = monitored_stream.writeStream \
    .foreachBatch(lambda df, epoch: monitor_batch(df, epoch)) \
    .start()
```

## Common Pitfalls

1. **Clock Skew**: Different systems may have slightly different clocks
2. **Network Delays**: Variable network latency affects event arrival
3. **Processing Time vs Event Time**: Always use event time for business logic
4. **State Accumulation**: Unbounded state growth without proper watermarking
5. **Join Explosion**: Cartesian products when join conditions are too loose

## Example Use Cases

- **IoT Sensor Correlation**: Different sensor types with varying frequencies
- **Financial Trading**: Order book updates vs trade executions
- **Web Analytics**: User interactions vs system performance metrics
- **Supply Chain**: RFID scans vs inventory updates
- **Gaming**: Player actions vs system events