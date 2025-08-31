from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, col, count, countDistinct, sum, to_date
import builtins
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from typing import Dict, Callable, Union, List, Tuple
import time
import random

# Type alias for partition analysis results
PartitionAnalysisResult = Dict[str, Union[int, float, str, List[int]]]

def create_partitioning_spark_session() -> SparkSession:
    """Create Spark session optimized for partitioning experiments"""
    return SparkSession.builder \
        .appName("PySpark Partitioning Strategies") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

class PartitionAnalyzer:
    """Utility class for analyzing and optimizing DataFrame partitions"""
    
    @staticmethod
    def analyze_partition_distribution(df: DataFrame, name: str = "DataFrame") -> PartitionAnalysisResult:
        """Comprehensive partition analysis"""
        print(f"\n=== Partition Analysis: {name} ===")
        
        num_partitions = df.rdd.getNumPartitions()
        partition_sizes = df.rdd.mapPartitionsWithIndex(
            lambda idx, iterator: [(idx, len(list(iterator)))]
        ).collect()
        
        sizes = [size for idx, size in partition_sizes]
        total_records = builtins.sum(sizes)
        
        if sizes:
            avg_size = total_records / len(sizes)
            max_size = builtins.max(sizes)
            min_size = builtins.min(sizes)
            skew_ratio = max_size / avg_size if avg_size > 0 else 0
        else:
            avg_size = max_size = min_size = skew_ratio = 0
        
        analysis = {
            "num_partitions": num_partitions,
            "total_records": total_records,
            "avg_partition_size": avg_size,
            "max_partition_size": max_size,
            "min_partition_size": min_size,
            "skew_ratio": skew_ratio,
            "partition_sizes": sizes
        }
        
        print(f"Partitions: {num_partitions}")
        print(f"Total records: {total_records}")
        print(f"Average partition size: {avg_size:.1f}")
        print(f"Max partition size: {max_size}")
        print(f"Min partition size: {min_size}")
        print(f"Skew ratio (max/avg): {skew_ratio:.2f}")
        
        # Visual representation of partition distribution
        if len(sizes) <= 20:  # Only show for reasonable number of partitions
            print("Partition sizes:", sizes)
        
        # Skew assessment
        if skew_ratio > 3:
            print("[WARNING] HIGH SKEW - Consider repartitioning!")
        elif skew_ratio > 1.5:
            print("[WARNING] Moderate skew detected")
        else:
            print("[SUCCESS] Well-balanced partitions")
        
        return analysis
    
    @staticmethod
    def show_partition_content_sample(df: DataFrame, max_partitions: int = 5) -> None:
        """Show sample content from each partition"""
        print(f"\n--- Partition Content Sample ---")
        
        def show_partition_info(idx, iterator):
            data = list(iterator)
            return [(idx, len(data), data[:3] if data else [])]  # First 3 records
        
        partition_info = df.rdd.mapPartitionsWithIndex(show_partition_info).collect()
        
        for idx, size, sample in partition_info[:max_partitions]:
            print(f"Partition {idx}: {size} records")
            if sample:
                for record in sample:
                    print(f"  Sample: {record}")
            print()

    @staticmethod
    def measure_operation_performance(df: DataFrame, operation: Callable, 
                                    operation_name: str) -> float:
        """Measure performance of an operation on a DataFrame"""
        start_time = time.time()
        result = operation(df)
        
        # Force execution
        if hasattr(result, 'count'):
            result.count()
        elif hasattr(result, 'collect'):
            result.collect()
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(f"[TIME] {operation_name}: {execution_time:.2f} seconds")
        return execution_time

def exercise_1_default_partitioning(spark: SparkSession) -> None:
    """Exercise 1: Understanding default partitioning behavior"""
    print("=== EXERCISE 1: Default Partitioning Behavior ===")
    print("Understand how Spark partitions data by default")
    
    # Create sample data
    data = [(i, f"user_{i}", random.randint(1, 100), f"city_{i % 10}") 
            for i in range(1, 10001)]
    
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("username", StringType(), False),
        StructField("score", IntegerType(), False),
        StructField("city", StringType(), False)
    ])
    
    df = spark.createDataFrame(data, schema)
    
    print("TODO: Analyze default partitioning:")
    print("1. Check default number of partitions")
    print("2. Understand partition size distribution")
    print("3. Observe how operations affect partitioning")
    
    # Default partitioning analysis
    print("\n--- Default DataFrame Partitioning ---")
    PartitionAnalyzer.analyze_partition_distribution(df, "Default DataFrame")
    
    # Show how operations change partitioning
    print("\n--- After Filter Operation ---")
    filtered_df = df.filter(col("score") > 50)
    PartitionAnalyzer.analyze_partition_distribution(filtered_df, "Filtered DataFrame")
    
    print("\n--- After GroupBy Operation ---")
    grouped_df = df.groupBy("city").count()
    PartitionAnalyzer.analyze_partition_distribution(grouped_df, "Grouped DataFrame")
    
    # Show partition content
    PartitionAnalyzer.show_partition_content_sample(df, 3)

def exercise_2_repartitioning_strategies(spark: SparkSession) -> None:
    """Exercise 2: Different repartitioning strategies"""
    print("\n=== EXERCISE 2: Repartitioning Strategies ===")
    print("Learn when and how to repartition data")
    
    # Load skewed data if available, otherwise create it
    try:
        # Try to load pre-generated skewed data
        df = spark.read.option("header", "true").csv("skewed_data/zipfian_skewed_sales.csv")
        print("Using pre-generated skewed sales data")
    except:
        # Create sample data with skew
        data = []
        # 80% of data for one city (skewed)
        for i in range(8000):
            data.append((i, f"user_{i}", "New York", random.randint(1, 100)))
        # 20% for other cities
        cities = ["LA", "Chicago", "Houston", "Phoenix"]
        for i in range(8000, 10000):
            data.append((i, f"user_{i}", random.choice(cities), random.randint(1, 100)))
        
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("username", StringType(), False),
            StructField("city", StringType(), False),
            StructField("score", IntegerType(), False)
        ])
        
        df = spark.createDataFrame(data, schema)
        print("Using generated sample data")
    
    print("\nTODO: Practice these repartitioning strategies:")
    print("1. Round-robin repartitioning")
    print("2. Hash-based repartitioning")
    print("3. Range-based repartitioning")
    print("4. Coalescing for reducing partitions")
    
    # Original partitioning
    print("\n--- Original Data Partitioning ---")
    original_analysis = PartitionAnalyzer.analyze_partition_distribution(df, "Original Data")
    
    # Strategy 1: Round-robin repartitioning
    print("\n--- Round-Robin Repartitioning (8 partitions) ---")
    repartitioned_rr = df.repartition(8)
    PartitionAnalyzer.analyze_partition_distribution(repartitioned_rr, "Round-Robin Repartitioned")
    
    # Strategy 2: Hash-based repartitioning by column
    print("\n--- Hash-Based Repartitioning (by city) ---")
    repartitioned_hash = df.repartition(col("city"))
    PartitionAnalyzer.analyze_partition_distribution(repartitioned_hash, "Hash Repartitioned by City")
    
    # Strategy 3: Coalescing (reduce partitions without full shuffle)
    print("\n--- Coalescing to 4 partitions ---")
    coalesced = df.coalesce(4)
    PartitionAnalyzer.analyze_partition_distribution(coalesced, "Coalesced")
    
    # Performance comparison
    print("\n--- Performance Comparison ---")
    
    def groupby_operation(dataframe):
        return dataframe.groupBy("city").agg(avg("price").alias("avg_price"))
    
    print("GroupBy performance comparison:")
    PartitionAnalyzer.measure_operation_performance(df, groupby_operation, "Original")
    PartitionAnalyzer.measure_operation_performance(repartitioned_hash, groupby_operation, "Hash Repartitioned")
    PartitionAnalyzer.measure_operation_performance(coalesced, groupby_operation, "Coalesced")

def exercise_3_bucketing_strategy(spark: SparkSession) -> None:
    """Exercise 3: Bucketing for join optimization"""
    print("\n=== EXERCISE 3: Bucketing Strategy ===")
    print("Use bucketing to optimize joins and avoid shuffles")
    
    print("TODO: Implement bucketing for join optimization:")
    print("1. Create bucketed tables")
    print("2. Compare bucketed vs non-bucketed joins")
    print("3. Understand bucket pruning benefits")
    
    # Create two datasets for joining
    customers_data = [(i, f"customer_{i}", f"city_{i % 20}", random.randint(18, 80)) 
                      for i in range(1, 5001)]
    orders_data = [(i, random.randint(1, 5000), f"product_{i % 100}", random.uniform(10, 500)) 
                   for i in range(1, 20001)]
    
    customers_schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("city", StringType(), False),
        StructField("age", IntegerType(), False)
    ])
    
    orders_schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("product", StringType(), False),
        StructField("amount", DoubleType(), False)
    ])
    
    customers_df = spark.createDataFrame(customers_data, customers_schema)
    orders_df = spark.createDataFrame(orders_data, orders_schema)
    
    print("\n--- Original DataFrames ---")
    PartitionAnalyzer.analyze_partition_distribution(customers_df, "Customers")
    PartitionAnalyzer.analyze_partition_distribution(orders_df, "Orders")
    
    # Demonstrate bucketing concept (simulated)
    print("\n--- Simulated Bucketing (by customer_id) ---")
    
    # Pre-partition both DataFrames by the join key
    bucketed_customers = customers_df.repartition(8, col("customer_id"))
    bucketed_orders = orders_df.repartition(8, col("customer_id"))
    
    PartitionAnalyzer.analyze_partition_distribution(bucketed_customers, "Bucketed Customers")
    PartitionAnalyzer.analyze_partition_distribution(bucketed_orders, "Bucketed Orders")
    
    # Compare join performance
    print("\n--- Join Performance Comparison ---")
    
    def join_operation(df1, df2):
        return df1.join(df2, "customer_id", "inner")
    
    print("Regular join:")
    regular_join_time = PartitionAnalyzer.measure_operation_performance(
        customers_df, lambda df: join_operation(df, orders_df), "Regular Join"
    )
    
    print("Bucketed join:")
    bucketed_join_time = PartitionAnalyzer.measure_operation_performance(
        bucketed_customers, lambda df: join_operation(df, bucketed_orders), "Bucketed Join"
    )
    
    # Show execution plans
    print("\n--- Execution Plans ---")
    regular_join = customers_df.join(orders_df, "customer_id", "inner")
    bucketed_join = bucketed_customers.join(bucketed_orders, "customer_id", "inner")
    
    print("Regular join plan:")
    regular_join.explain()
    
    print("\nBucketed join plan:")
    bucketed_join.explain()

def exercise_4_range_partitioning(spark: SparkSession) -> None:
    """Exercise 4: Range partitioning for ordered data"""
    print("\n=== EXERCISE 4: Range Partitioning ===")
    print("Use range partitioning for time-series and ordered data")
    
    # Create time-series data
    from datetime import datetime, timedelta
    import random
    
    base_date = datetime(2024, 1, 1)
    timeseries_data = []
    
    for i in range(10000):
        date = base_date + timedelta(days=random.randint(0, 365))
        timeseries_data.append((
            i,
            date.strftime("%Y-%m-%d"),
            f"sensor_{random.randint(1, 10)}",
            random.uniform(10, 100),
            random.randint(0, 1000)
        ))
    
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("date", StringType(), False),
        StructField("sensor_id", StringType(), False),
        StructField("temperature", DoubleType(), False),
        StructField("reading", IntegerType(), False)
    ])
    
    df = spark.createDataFrame(timeseries_data, schema)
    df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    
    print("TODO: Implement range partitioning strategies:")
    print("1. Partition by date ranges")
    print("2. Partition by value ranges")
    print("3. Optimize for range queries")
    
    print("\n--- Original Time Series Data ---")
    PartitionAnalyzer.analyze_partition_distribution(df, "Original Time Series")
    
    # Range partitioning by date (simulated with multiple repartition calls)
    print("\n--- Range Partitioning by Date ---")
    
    # Split data into quarterly ranges
    q1_df = df.filter((col("date") >= "2024-01-01") & (col("date") < "2024-04-01"))
    q2_df = df.filter((col("date") >= "2024-04-01") & (col("date") < "2024-07-01"))
    q3_df = df.filter((col("date") >= "2024-07-01") & (col("date") < "2024-10-01"))
    q4_df = df.filter((col("date") >= "2024-10-01") & (col("date") <= "2024-12-31"))
    
    # Coalesce each range and union
    range_partitioned = q1_df.coalesce(1).union(
        q2_df.coalesce(1)).union(
        q3_df.coalesce(1)).union(
        q4_df.coalesce(1))
    
    PartitionAnalyzer.analyze_partition_distribution(range_partitioned, "Range Partitioned by Date")
    
    # Show partition content to verify range partitioning
    PartitionAnalyzer.show_partition_content_sample(range_partitioned, 4)
    
    # Performance test for range queries
    print("\n--- Range Query Performance Test ---")
    
    def range_query(dataframe):
        return dataframe.filter(
            (col("date") >= "2024-06-01") & (col("date") <= "2024-08-31")
        ).agg(avg("temperature").alias("avg_temp"))
    
    print("Range query performance:")
    PartitionAnalyzer.measure_operation_performance(df, range_query, "Original Data")
    PartitionAnalyzer.measure_operation_performance(range_partitioned, range_query, "Range Partitioned")

def exercise_5_dynamic_partitioning(spark: SparkSession) -> None:
    """Exercise 5: Dynamic partitioning based on data characteristics"""
    print("\n=== EXERCISE 5: Dynamic Partitioning ===")
    print("Implement dynamic partitioning based on data analysis")
    
    # Create sample data with varying characteristics
    mixed_data = []
    
    # Different data patterns
    patterns = {
        "high_volume": (5000, 100),   # 5000 records, 100 distinct values
        "medium_volume": (2000, 50),  # 2000 records, 50 distinct values
        "low_volume": (500, 10)       # 500 records, 10 distinct values
    }
    
    record_id = 1
    for pattern_name, (num_records, distinct_values) in patterns.items():
        for i in range(num_records):
            mixed_data.append((
                record_id,
                pattern_name,
                f"value_{i % distinct_values}",
                random.uniform(1, 1000),
                random.randint(1, 100)
            ))
            record_id += 1
    
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("pattern_type", StringType(), False),
        StructField("category", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("quantity", IntegerType(), False)
    ])
    
    df = spark.createDataFrame(mixed_data, schema)
    
    print("TODO: Implement dynamic partitioning:")
    print("1. Analyze data characteristics")
    print("2. Determine optimal partition strategy")
    print("3. Apply different strategies to different data patterns")
    
    print("\n--- Data Pattern Analysis ---")
    
    # Analyze each pattern
    pattern_stats = df.groupBy("pattern_type").agg(
        count("*").alias("record_count"),
        countDistinct("category").alias("distinct_categories"),
        avg("amount").alias("avg_amount")
    ).collect()
    
    for row in pattern_stats:
        print(f"Pattern: {row['pattern_type']}")
        print(f"  Records: {row['record_count']}")
        print(f"  Distinct categories: {row['distinct_categories']}")
        print(f"  Avg amount: {row['avg_amount']:.2f}")
    
    # Dynamic partitioning strategy
    print("\n--- Applying Dynamic Partitioning ---")
    
    # High volume data: Partition by category for parallel processing
    high_volume_df = df.filter(col("pattern_type") == "high_volume").repartition(col("category"))
    
    # Medium volume data: Moderate partitioning
    medium_volume_df = df.filter(col("pattern_type") == "medium_volume").repartition(4)
    
    # Low volume data: Coalesce to single partition
    low_volume_df = df.filter(col("pattern_type") == "low_volume").coalesce(1)
    
    # Analyze partitioning results
    PartitionAnalyzer.analyze_partition_distribution(high_volume_df, "High Volume (by category)")
    PartitionAnalyzer.analyze_partition_distribution(medium_volume_df, "Medium Volume (4 partitions)")
    PartitionAnalyzer.analyze_partition_distribution(low_volume_df, "Low Volume (coalesced)")
    
    # Combine optimized partitions
    optimized_df = high_volume_df.union(medium_volume_df).union(low_volume_df)
    PartitionAnalyzer.analyze_partition_distribution(optimized_df, "Dynamically Optimized")

def exercise_6_partition_pruning(spark: SparkSession) -> None:
    """Exercise 6: Understanding and leveraging partition pruning"""
    print("\n=== EXERCISE 6: Partition Pruning ===")
    print("Optimize queries through effective partition pruning")
    
    print("TODO: Understand partition pruning benefits:")
    print("1. Create partitioned data structure")
    print("2. Write queries that benefit from pruning")
    print("3. Compare pruned vs non-pruned query performance")
    
    # Create data with clear partitioning structure
    partitioned_data = []
    regions = ["North", "South", "East", "West"]
    
    for region in regions:
        for i in range(2500):  # 2500 records per region
            partitioned_data.append((
                len(partitioned_data) + 1,
                region,
                f"store_{i % 50}",
                f"product_{i % 100}",
                random.uniform(10, 500),
                f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
            ))
    
    schema = StructType([
        StructField("sale_id", IntegerType(), False),
        StructField("region", StringType(), False),
        StructField("store", StringType(), False),
        StructField("product", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("date", StringType(), False)
    ])
    
    df = spark.createDataFrame(partitioned_data, schema)
    
    # Partition by region
    partitioned_by_region = df.repartition(col("region"))
    
    print("\n--- Partitioned Data Analysis ---")
    PartitionAnalyzer.analyze_partition_distribution(partitioned_by_region, "Partitioned by Region")
    PartitionAnalyzer.show_partition_content_sample(partitioned_by_region, 4)
    
    # Demonstrate partition pruning with queries
    print("\n--- Partition Pruning Demonstration ---")
    
    def region_specific_query(dataframe):
        return dataframe.filter(col("region") == "North").agg(
            sum("amount").alias("total_sales"),
            count("*").alias("transaction_count")
        )
    
    def multi_region_query(dataframe):
        return dataframe.filter(col("region").isin(["North", "South"])).groupBy("region").agg(
            avg("amount").alias("avg_sale_amount")
        )
    
    print("Single region query (benefits from pruning):")
    PartitionAnalyzer.measure_operation_performance(df, region_specific_query, "Non-partitioned")
    PartitionAnalyzer.measure_operation_performance(partitioned_by_region, region_specific_query, "Partitioned")
    
    print("\nMulti-region query:")
    PartitionAnalyzer.measure_operation_performance(df, multi_region_query, "Non-partitioned")
    PartitionAnalyzer.measure_operation_performance(partitioned_by_region, multi_region_query, "Partitioned")
    
    # Show execution plans
    print("\n--- Execution Plans Comparison ---")
    print("Non-partitioned query plan:")
    region_specific_query(df).explain()
    
    print("\nPartitioned query plan:")
    region_specific_query(partitioned_by_region).explain()

def main():
    """Run all partitioning strategy exercises"""
    spark = create_partitioning_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("PySpark Partitioning Strategies and Optimization")
    print("=" * 60)
    
    try:
        exercise_1_default_partitioning(spark)
        exercise_2_repartitioning_strategies(spark)
        exercise_3_bucketing_strategy(spark)
        exercise_4_range_partitioning(spark)
        exercise_5_dynamic_partitioning(spark)
        exercise_6_partition_pruning(spark)
        
        print("\n" + "=" * 60)
        print("[COMPLETE] Partitioning Strategy Exercises Complete!")
        print("\nKey Partitioning Best Practices:")
        print("[TIP] Partition by frequently filtered columns")
        print("[TIP] Use appropriate number of partitions (not too many, not too few)")
        print("[TIP] Consider bucketing for frequently joined tables")
        print("[TIP] Use range partitioning for time-series data")
        print("[TIP] Leverage partition pruning for query optimization")
        print("[TIP] Monitor partition skew and rebalance when needed")
        print("[TIP] Use coalesce() to reduce small partitions")
        print("[TIP] Apply dynamic partitioning based on data characteristics")
        
    except Exception as e:
        print(f"Error in exercises: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()