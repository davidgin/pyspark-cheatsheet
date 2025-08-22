from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
import builtins
from pyspark.sql.types import *
from typing import Callable, TypeVar, ParamSpec
import time
import random

# Type variables for better decorator typing
P = ParamSpec('P')
R = TypeVar('R')

def create_optimized_spark_session() -> SparkSession:
    """Create Spark session with optimization configurations"""
    return SparkSession.builder \
        .appName("PySpark Shuffle and Skew Optimization") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def measure_execution_time(func: Callable[P, R]) -> Callable[P, R]:
    """Decorator to measure execution time"""
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"⏱️  Execution time: {end_time - start_time:.2f} seconds")
        return result
    return wrapper

class ShuffleAnalyzer:
    """Utility class to analyze shuffle operations"""
    
    @staticmethod
    def analyze_partitions(df: DataFrame, name: str = "DataFrame") -> None:
        """Analyze partition distribution"""
        print(f"\n=== Partition Analysis for {name} ===")
        print(f"Number of partitions: {df.rdd.getNumPartitions()}")
        
        # Get records per partition
        partition_counts = df.rdd.mapPartitionsWithIndex(
            lambda idx, iterator: [len(list(iterator))]
        ).collect()
        
        print(f"Records per partition: {partition_counts}")
        print(f"Total records: {builtins.sum(partition_counts)}")
        print(f"Max partition size: {builtins.max(partition_counts)}")
        print(f"Min partition size: {builtins.min(partition_counts)}")
        
        # Check for skew
        if partition_counts:
            avg_size = builtins.sum(partition_counts) / len(partition_counts)
            max_skew_ratio = builtins.max(partition_counts) / avg_size if avg_size > 0 else 0
            print(f"Skew ratio (max/avg): {max_skew_ratio:.2f}")
            
            if max_skew_ratio > 3:
                print("⚠️  HIGH SKEW DETECTED! Consider repartitioning.")
            elif max_skew_ratio > 1.5:
                print("⚠️  Moderate skew detected.")
            else:
                print("✅ Well-balanced partitions.")

    @staticmethod
    def show_execution_plan(df: DataFrame, name: str = "Query") -> None:
        """Show the execution plan to identify shuffles"""
        print(f"\n=== Execution Plan for {name} ===")
        df.explain(extended=True)

def exercise_1_shuffle_detection(spark: SparkSession) -> None:
    """Exercise 1: Detecting shuffle operations"""
    print("=== EXERCISE 1: Shuffle Detection ===")
    print("Learn to identify operations that trigger shuffles")
    
    # Create sample data
    data1 = [(i, f"user_{i}", random.randint(1, 100)) for i in range(1, 1001)]
    data2 = [(i, f"product_{i}", random.uniform(10, 500)) for i in range(1, 501)]
    
    schema1 = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("username", StringType(), False),
        StructField("age", IntegerType(), False)
    ])
    
    schema2 = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), False),
        StructField("price", DoubleType(), False)
    ])
    
    df_users = spark.createDataFrame(data1, schema1)
    df_products = spark.createDataFrame(data2, schema2)
    
    print("\nTODO: Analyze these operations for shuffle behavior:")
    print("1. groupBy() operations")
    print("2. join() operations")
    print("3. orderBy() operations")
    print("4. distinct() operations")
    
    # Example 1: GroupBy (triggers shuffle)
    print("\n--- GroupBy Operation (Shuffle) ---")
    grouped_df = df_users.groupBy("age").count()
    ShuffleAnalyzer.show_execution_plan(grouped_df, "GroupBy Age")
    ShuffleAnalyzer.analyze_partitions(grouped_df, "Grouped by Age")
    
    # Example 2: Filter (no shuffle)
    print("\n--- Filter Operation (No Shuffle) ---")
    filtered_df = df_users.filter(col("age") > 50)
    ShuffleAnalyzer.show_execution_plan(filtered_df, "Filter Age > 50")
    
    # Uncomment to run:
    # grouped_df.show(10)
    # filtered_df.show(10)

def exercise_2_join_optimization(spark: SparkSession) -> None:
    """Exercise 2: Join optimization strategies"""
    print("\n=== EXERCISE 2: Join Optimization ===")
    print("Optimize joins to reduce shuffle operations")
    
    # Create datasets with different sizes
    large_data = [(i, f"user_{i}", random.randint(18, 80), f"city_{random.randint(1, 50)}") 
                  for i in range(1, 10001)]
    small_data = [(i, f"product_{i}", random.uniform(10, 1000)) 
                  for i in range(1, 101)]
    
    large_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("city", StringType(), False)
    ])
    
    small_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("product", StringType(), False),
        StructField("price", DoubleType(), False)
    ])
    
    large_df = spark.createDataFrame(large_data, large_schema)
    small_df = spark.createDataFrame(small_data, small_schema)
    
    print("TODO: Practice these join optimization techniques:")
    print("1. Broadcast joins for small tables")
    print("2. Bucket joins for frequently joined tables")
    print("3. Salting for skewed joins")
    
    # Broadcast join (no shuffle for small table)
    print("\n--- Broadcast Join ---")
    broadcast_join = large_df.join(broadcast(small_df), "id", "inner")
    ShuffleAnalyzer.show_execution_plan(broadcast_join, "Broadcast Join")
    
    # Regular join (shuffle both sides)
    print("\n--- Regular Join ---")
    regular_join = large_df.join(small_df, "id", "inner")
    ShuffleAnalyzer.show_execution_plan(regular_join, "Regular Join")
    
    # Uncomment to compare performance:
    # measure_broadcast_join(large_df, small_df)
    # measure_regular_join(large_df, small_df)

@measure_execution_time
def measure_broadcast_join(large_df: DataFrame, small_df: DataFrame) -> None:
    """Measure broadcast join performance"""
    result = large_df.join(broadcast(small_df), "id", "inner")
    result.count()  # Trigger execution

@measure_execution_time
def measure_regular_join(large_df: DataFrame, small_df: DataFrame) -> None:
    """Measure regular join performance"""
    result = large_df.join(small_df, "id", "inner")
    result.count()  # Trigger execution

def exercise_3_partitioning_strategies(spark: SparkSession) -> None:
    """Exercise 3: Partitioning strategies to reduce shuffles"""
    print("\n=== EXERCISE 3: Partitioning Strategies ===")
    print("Use partitioning to optimize data layout")
    
    # Create sample sales data
    sales_data = []
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
    for i in range(1, 5001):
        sales_data.append((
            i, 
            random.choice(cities),
            f"product_{random.randint(1, 100)}",
            random.randint(1, 10),
            random.uniform(10, 500),
            f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        ))
    
    sales_schema = StructType([
        StructField("sale_id", IntegerType(), False),
        StructField("city", StringType(), False),
        StructField("product", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("amount", DoubleType(), False),
        StructField("sale_date", StringType(), False)
    ])
    
    sales_df = spark.createDataFrame(sales_data, sales_schema)
    
    print("TODO: Practice these partitioning strategies:")
    print("1. Partition by frequently filtered columns")
    print("2. Repartition to balance load")
    print("3. Coalesce to reduce small partitions")
    
    # Analyze original partitioning
    print("\n--- Original Partitioning ---")
    ShuffleAnalyzer.analyze_partitions(sales_df, "Original Sales Data")
    
    # Partition by city (common filter column)
    print("\n--- Partitioned by City ---")
    partitioned_df = sales_df.repartition(col("city"))
    ShuffleAnalyzer.analyze_partitions(partitioned_df, "Partitioned by City")
    
    # Repartition to specific number
    print("\n--- Repartitioned to 8 partitions ---")
    repartitioned_df = sales_df.repartition(8)
    ShuffleAnalyzer.analyze_partitions(repartitioned_df, "Repartitioned to 8")
    
    # Coalesce to reduce partitions
    print("\n--- Coalesced to 4 partitions ---")
    coalesced_df = sales_df.coalesce(4)
    ShuffleAnalyzer.analyze_partitions(coalesced_df, "Coalesced to 4")

def exercise_4_skew_detection(spark: SparkSession) -> None:
    """Exercise 4: Detecting and handling data skew"""
    print("\n=== EXERCISE 4: Data Skew Detection ===")
    print("Identify and mitigate data skew issues")
    
    # Create skewed data (80% of data goes to one key)
    skewed_data = []
    
    # 80% of records have the same key (creating skew)
    for i in range(8000):
        skewed_data.append(("popular_product", f"user_{i}", random.uniform(10, 100)))
    
    # 20% distributed among other keys
    other_products = [f"product_{j}" for j in range(1, 21)]
    for i in range(2000):
        skewed_data.append((random.choice(other_products), f"user_{i+8000}", random.uniform(10, 100)))
    
    skewed_schema = StructType([
        StructField("product_key", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("amount", DoubleType(), False)
    ])
    
    skewed_df = spark.createDataFrame(skewed_data, skewed_schema)
    
    print("TODO: Practice skew detection and mitigation:")
    print("1. Identify skewed keys")
    print("2. Use salting technique")
    print("3. Apply isolated broadcast maps")
    print("4. Separate processing for hot keys")
    
    # Detect skew by analyzing key distribution
    print("\n--- Key Distribution Analysis ---")
    key_distribution = skewed_df.groupBy("product_key").count().orderBy(desc("count"))
    key_distribution.show(20)
    
    # Analyze partitions after groupBy (will show skew)
    print("\n--- Partition Analysis after GroupBy ---")
    grouped_skewed = skewed_df.groupBy("product_key").agg(
        sum("amount").alias("total_amount"),
        count("*").alias("record_count")
    )
    ShuffleAnalyzer.analyze_partitions(grouped_skewed, "Skewed GroupBy Result")

def exercise_5_salting_technique(spark: SparkSession) -> None:
    """Exercise 5: Salting technique to handle skew"""
    print("\n=== EXERCISE 5: Salting Technique ===")
    print("Use salting to distribute skewed keys")
    
    # Create skewed data
    skewed_data = []
    
    # Create extreme skew - 90% on one key
    hot_key = "hot_key"
    for i in range(9000):
        skewed_data.append((hot_key, f"value_{i}", random.randint(1, 100)))
    
    # 10% on other keys
    for i in range(1000):
        skewed_data.append((f"key_{i % 100}", f"value_{i+9000}", random.randint(1, 100)))
    
    schema = StructType([
        StructField("key", StringType(), False),
        StructField("value", StringType(), False),
        StructField("amount", IntegerType(), False)
    ])
    
    df = spark.createDataFrame(skewed_data, schema)
    
    print("TODO: Implement salting to resolve skew:")
    print("1. Add salt to skewed keys")
    print("2. Perform operation on salted data")
    print("3. Remove salt and aggregate final results")
    
    # Step 1: Identify hot keys (in practice, you'd do this analysis first)
    hot_keys = ["hot_key"]  # Known from analysis
    
    # Step 2: Apply salting
    salt_range = 10  # Number of salt values
    
    salted_df = df.withColumn(
        "salted_key",
        when(col("key").isin(hot_keys),
             concat(col("key"), lit("_salt_"), 
                   (rand() * salt_range).cast("int").cast("string")))
        .otherwise(col("key"))
    )
    
    print("\n--- Before Salting ---")
    original_grouped = df.groupBy("key").count()
    ShuffleAnalyzer.analyze_partitions(original_grouped, "Original Grouped")
    
    print("\n--- After Salting ---")
    salted_grouped = salted_df.groupBy("salted_key").agg(
        sum("amount").alias("total_amount"),
        count("*").alias("record_count")
    )
    ShuffleAnalyzer.analyze_partitions(salted_grouped, "Salted Grouped")
    
    # Step 3: Final aggregation (remove salt)
    final_result = salted_grouped.withColumn(
        "original_key",
        when(col("salted_key").contains("_salt_"),
             split(col("salted_key"), "_salt_").getItem(0))
        .otherwise(col("salted_key"))
    ).groupBy("original_key").agg(
        sum("total_amount").alias("final_total"),
        sum("record_count").alias("final_count")
    )
    
    print("\n--- Final Aggregated Results ---")
    final_result.show()

def exercise_6_adaptive_query_execution(spark: SparkSession) -> None:
    """Exercise 6: Understanding Adaptive Query Execution (AQE)"""
    print("\n=== EXERCISE 6: Adaptive Query Execution ===")
    print("Leverage AQE for automatic optimization")
    
    print("TODO: Understand AQE features:")
    print("1. Adaptive partition coalescing")
    print("2. Adaptive skew join optimization")
    print("3. Dynamic join strategy switching")
    
    # Check AQE settings
    print("\n--- Current AQE Configuration ---")
    aqe_configs = [
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.sql.adaptive.skewJoin.enabled",
        "spark.sql.adaptive.localShuffleReader.enabled"
    ]
    
    for config in aqe_configs:
        value = spark.conf.get(config, "Not Set")
        print(f"{config}: {value}")
    
    # Create example for AQE demonstration
    large_data = [(i, f"user_{i}", i % 100, random.uniform(10, 1000)) 
                  for i in range(1, 10001)]
    
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("group_id", IntegerType(), False),
        StructField("value", DoubleType(), False)
    ])
    
    df = spark.createDataFrame(large_data, schema)
    
    # This will benefit from AQE optimizations
    result = df.groupBy("group_id").agg(
        avg("value").alias("avg_value"),
        count("*").alias("count")
    ).filter(col("count") > 50)
    
    print("\n--- Query with AQE Optimizations ---")
    ShuffleAnalyzer.show_execution_plan(result, "AQE Optimized Query")
    
    print("\n💡 AQE Benefits:")
    print("   - Automatically coalesces small partitions")
    print("   - Handles skewed joins dynamically")
    print("   - Switches join strategies based on runtime stats")
    print("   - Reduces shuffle overhead")

def exercise_7_performance_comparison(spark: SparkSession) -> None:
    """Exercise 7: Performance comparison of optimization techniques"""
    print("\n=== EXERCISE 7: Performance Comparison ===")
    print("Compare performance of different optimization approaches")
    
    # Create test dataset
    test_data = [(i, f"category_{i % 20}", f"product_{i}", random.uniform(10, 500)) 
                 for i in range(1, 50001)]
    
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("category", StringType(), False),
        StructField("product", StringType(), False),
        StructField("price", DoubleType(), False)
    ])
    
    df = spark.createDataFrame(test_data, schema)
    
    print("TODO: Compare these optimization techniques:")
    print("1. Default behavior vs optimized")
    print("2. Different partitioning strategies")
    print("3. With and without broadcast hints")
    
    # Test 1: Default aggregation
    @measure_execution_time
    def test_default_aggregation():
        result = df.groupBy("category").agg(
            avg("price").alias("avg_price"),
            count("*").alias("product_count")
        )
        return result.collect()
    
    # Test 2: Pre-partitioned aggregation
    @measure_execution_time
    def test_partitioned_aggregation():
        partitioned_df = df.repartition(col("category"))
        result = partitioned_df.groupBy("category").agg(
            avg("price").alias("avg_price"),
            count("*").alias("product_count")
        )
        return result.collect()
    
    print("\n--- Performance Comparison ---")
    print("Default aggregation:")
    test_default_aggregation()
    
    print("\nPre-partitioned aggregation:")
    test_partitioned_aggregation()
    
    print("\n💡 Optimization Tips:")
    print("   - Use appropriate partitioning for your access patterns")
    print("   - Enable AQE for automatic optimizations")
    print("   - Monitor execution plans for shuffle operations")
    print("   - Use broadcast joins for small dimension tables")
    print("   - Apply salting for highly skewed data")

def main():
    """Run all shuffle optimization exercises"""
    spark = create_optimized_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("PySpark Shuffle Optimization and Skew Handling Exercises")
    print("=" * 70)
    
    try:
        exercise_1_shuffle_detection(spark)
        exercise_2_join_optimization(spark)
        exercise_3_partitioning_strategies(spark)
        exercise_4_skew_detection(spark)
        exercise_5_salting_technique(spark)
        exercise_6_adaptive_query_execution(spark)
        exercise_7_performance_comparison(spark)
        
        print("\n" + "=" * 70)
        print("🎯 Shuffle Optimization Exercises Complete!")
        print("\nKey Takeaways:")
        print("✅ Use explicit partitioning for frequently accessed columns")
        print("✅ Enable AQE for automatic optimizations")
        print("✅ Apply broadcast joins for small tables")
        print("✅ Use salting technique for skewed data")
        print("✅ Monitor partition distribution and execution plans")
        
    except Exception as e:
        print(f"Error in exercises: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()