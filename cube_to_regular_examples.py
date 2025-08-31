from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, cast, col, count, countDistinct, cube, desc, filter, lag, lit, max, min, month, quarter, rank, rollup, sum, to_date, union, withColumn, year
from pyspark.sql.functions import round as spark_round
import builtins
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
import random

def create_spark_session() -> SparkSession:
    """Create Spark session for cube vs regular table examples"""
    return SparkSession.builder \
        .appName("Cube vs Regular Table Operations") \
        .master("local[*]") \
        .getOrCreate()

def create_sample_sales_data(spark: SparkSession) -> DataFrame:
    """Create sample sales data for demonstration"""
    data = []
    regions = ["North", "South", "East", "West"]
    categories = ["Electronics", "Clothing", "Books", "Home"]
    products = {
        "Electronics": ["Laptop", "Phone", "Tablet"],
        "Clothing": ["Shirt", "Pants", "Dress"],
        "Books": ["Novel", "Textbook", "Magazine"],
        "Home": ["Chair", "Table", "Lamp"]
    }
    
    for i in range(1000):
        region = random.choice(regions)
        category = random.choice(categories)
        product = random.choice(products[category])
        
        data.append((
            i + 1,
            region,
            category, 
            product,
            random.randint(1, 10),  # quantity
            builtins.round(random.uniform(10, 500), 2),  # price
            f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        ))
    
    schema = StructType([
        StructField("sale_id", IntegerType(), False),
        StructField("region", StringType(), False),
        StructField("category", StringType(), False),
        StructField("product", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("price", DoubleType(), False),
        StructField("sale_date", StringType(), False)
    ])
    
    df = spark.createDataFrame(data, schema)
    df = df.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))
    df = df.withColumn("revenue", col("quantity") * col("price"))
    
    return df

def example_1_cube_vs_regular_aggregation(spark: SparkSession) -> None:
    """Example 1: Replace cube() with regular groupBy() operations"""
    print("=== EXAMPLE 1: Cube vs Regular Aggregation ===")
    
    try:
        print("Creating sample data...")
        df = create_sample_sales_data(spark)
        print("[SUCCESS] Sample data created successfully")
    except Exception as e:
        print(f"[ERROR] Error creating sample data: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("Sample data:")
    df.show(5)
    
    print("\n--- Using CUBE operation (creates all combinations) ---")
    # CUBE operation - creates subtotals for all combinations
    cube_result = df.cube("region", "category").agg(
        sum("revenue").alias("total_revenue"),
        count("*").alias("total_sales")
    ).orderBy("region", "category")
    
    print("Cube results (includes subtotals and grand total):")
    cube_result.show(20, truncate=False)
    
    print(f"Cube result count: {cube_result.count()} rows")
    
    print("\n--- Using REGULAR TABLE operations (explicit aggregations) ---")
    
    # Replace cube with explicit regular aggregations
    
    # 1. Detailed breakdown by region and category
    detailed_agg = df.groupBy("region", "category").agg(
        sum("revenue").alias("total_revenue"),
        count("*").alias("total_sales")
    ).withColumn("aggregation_level", lit("Region_Category"))
    
    # 2. Subtotal by region only  
    region_agg = df.groupBy("region").agg(
        sum("revenue").alias("total_revenue"),
        count("*").alias("total_sales")
    ).withColumn("category", lit(None).cast(StringType())) \
     .withColumn("aggregation_level", lit("Region_Only"))
    
    # 3. Subtotal by category only
    category_agg = df.groupBy("category").agg(
        sum("revenue").alias("total_revenue"),
        count("*").alias("total_sales")
    ).withColumn("region", lit(None).cast(StringType())) \
     .withColumn("aggregation_level", lit("Category_Only")) \
     .select("region", "category", "total_revenue", "total_sales", "aggregation_level")
    
    # 4. Grand total
    grand_total = df.agg(
        sum("revenue").alias("total_revenue"),
        count("*").alias("total_sales")
    ).withColumn("region", lit(None).cast(StringType())) \
     .withColumn("category", lit(None).cast(StringType())) \
     .withColumn("aggregation_level", lit("Grand_Total")) \
     .select("region", "category", "total_revenue", "total_sales", "aggregation_level")
    
    # Union all regular aggregations to simulate cube behavior
    regular_result = detailed_agg \
        .union(region_agg) \
        .union(category_agg) \
        .union(grand_total) \
        .orderBy("region", "category", "aggregation_level")
    
    print("Regular aggregation results (explicit subtotals):")
    regular_result.show(25, truncate=False)
    
    print(f"Regular result count: {regular_result.count()} rows")
    
    print("\n[BENEFITS] Benefits of Regular Tables over Cube:")
    print("   - More explicit and readable")
    print("   - Better control over aggregation levels")
    print("   - Easier to add business logic")
    print("   - More predictable performance")
    print("   - Easier to debug and maintain")

def example_2_rollup_vs_regular_hierarchy(spark: SparkSession) -> None:
    """Example 2: Replace rollup() with regular hierarchical aggregations"""
    print("\n=== EXAMPLE 2: Rollup vs Regular Hierarchy ===")
    
    df = create_sample_sales_data(spark)
    
    print("--- Using ROLLUP operation (hierarchical subtotals) ---")
    # ROLLUP creates hierarchical subtotals: (region, category), (region), ()
    rollup_result = df.rollup("region", "category").agg(
        sum("revenue").alias("total_revenue"),
        avg("price").alias("avg_price")
    ).orderBy("region", "category")
    
    print("Rollup results:")
    rollup_result.show(20, truncate=False)
    
    print("\n--- Using REGULAR TABLE operations (step-by-step hierarchy) ---")
    
    # Replace rollup with explicit hierarchical aggregations
    
    # Level 1: Most detailed - Region + Category
    level1_agg = df.groupBy("region", "category").agg(
        sum("revenue").alias("total_revenue"),
        avg("price").alias("avg_price")
    ).withColumn("hierarchy_level", lit("Level_1_Region_Category"))
    
    # Level 2: Intermediate - Region only
    level2_agg = df.groupBy("region").agg(
        sum("revenue").alias("total_revenue"),
        avg("price").alias("avg_price")
    ).withColumn("category", lit(None).cast(StringType())) \
     .withColumn("hierarchy_level", lit("Level_2_Region_Only"))
    
    # Level 3: Top level - Grand total
    level3_agg = df.agg(
        sum("revenue").alias("total_revenue"),
        avg("price").alias("avg_price")
    ).withColumn("region", lit(None).cast(StringType())) \
     .withColumn("category", lit(None).cast(StringType())) \
     .withColumn("hierarchy_level", lit("Level_3_Grand_Total"))
    
    # Combine all levels
    regular_hierarchy = level1_agg \
        .union(level2_agg) \
        .union(level3_agg) \
        .select("region", "category", "total_revenue", "avg_price", "hierarchy_level") \
        .orderBy("hierarchy_level", "region", "category")
    
    print("Regular hierarchy results:")
    regular_hierarchy.show(25, truncate=False)
    
    print("\n[ADVANTAGES] Advantages of Regular Hierarchy:")
    print("   - Clear separation of aggregation levels")
    print("   - Easy to add custom logic per level")
    print("   - Better performance control")
    print("   - Simpler to understand and modify")

def example_3_multidimensional_regular_analysis(spark: SparkSession) -> None:
    """Example 3: Multi-dimensional analysis using regular tables"""
    print("\n=== EXAMPLE 3: Multi-dimensional Analysis with Regular Tables ===")
    
    df = create_sample_sales_data(spark)
    
    # Add time dimensions
    df = df.withColumn("year", year(col("sale_date"))) \
           .withColumn("month", month(col("sale_date"))) \
           .withColumn("quarter", quarter(col("sale_date")))
    
    print("Enhanced data with time dimensions:")
    df.select("region", "category", "year", "month", "quarter", "revenue").show(5)
    
    print("\n--- Multi-dimensional Analysis using Regular Operations ---")
    
    # Dimension 1: Time-based analysis
    print("1. Time-based Analysis:")
    time_analysis = df.groupBy("year", "quarter", "month").agg(
        sum("revenue").alias("total_revenue"),
        count("*").alias("sales_count"),
        countDistinct("region").alias("regions_active")
    ).orderBy("year", "quarter", "month")
    
    time_analysis.show(10)
    
    # Dimension 2: Geographic analysis
    print("\n2. Geographic Analysis:")
    geo_analysis = df.groupBy("region", "category").agg(
        sum("revenue").alias("total_revenue"),
        avg("price").alias("avg_price"),
        sum("quantity").alias("total_quantity")
    ).orderBy(desc("total_revenue"))
    
    geo_analysis.show(10)
    
    # Dimension 3: Product performance analysis
    print("\n3. Product Performance Analysis:")
    product_analysis = df.groupBy("category", "product").agg(
        sum("revenue").alias("total_revenue"),
        count("*").alias("sales_frequency"),
        avg("quantity").alias("avg_quantity_per_sale")
    ).orderBy("category", desc("total_revenue"))
    
    product_analysis.show(15)
    
    # Dimension 4: Cross-dimensional insights
    print("\n4. Cross-dimensional Insights (Region + Time):")
    cross_analysis = df.groupBy("region", "quarter").agg(
        sum("revenue").alias("quarterly_revenue"),
        countDistinct("category").alias("categories_sold"),
        avg("price").alias("avg_price")
    ).withColumn("revenue_per_category", 
                spark_round(col("quarterly_revenue") / col("categories_sold"), 2)) \
     .orderBy("region", "quarter")
    
    cross_analysis.show(20)
    
    print("\n[BENEFITS] Regular Multi-dimensional Benefits:")
    print("   - Each analysis serves a specific business purpose")
    print("   - Easy to optimize each query independently")
    print("   - Clear business meaning for each result")
    print("   - Can add different filters/conditions per dimension")
    print("   - Better suited for BI tools and dashboards")

def example_4_performance_comparison(spark: SparkSession) -> None:
    """Example 4: Performance comparison cube vs regular operations"""
    print("\n=== EXAMPLE 4: Performance Comparison ===")
    
    df = create_sample_sales_data(spark)
    
    import time
    
    # Cube operation timing
    print("Testing CUBE operation performance...")
    start_time = time.time()
    cube_result = df.cube("region", "category", "product").agg(
        sum("revenue").alias("total_revenue")
    )
    cube_count = cube_result.count()  # Force execution
    cube_time = time.time() - start_time
    
    print(f"Cube operation: {cube_time:.2f} seconds, {cube_count} result rows")
    
    # Regular operations timing
    print("\nTesting REGULAR operations performance...")
    start_time = time.time()
    
    # Just get the specific aggregations we actually need
    region_totals = df.groupBy("region").agg(sum("revenue").alias("total_revenue"))
    category_totals = df.groupBy("category").agg(sum("revenue").alias("total_revenue"))
    product_totals = df.groupBy("product").agg(sum("revenue").alias("total_revenue"))
    
    # Force execution
    regular_count = (region_totals.count() + 
                    category_totals.count() + 
                    product_totals.count())
    
    regular_time = time.time() - start_time
    
    print(f"Regular operations: {regular_time:.2f} seconds, {regular_count} result rows")
    
    print(f"\n[PERFORMANCE] Performance Summary:")
    print(f"   Cube: {cube_time:.2f}s for {cube_count} rows")
    print(f"   Regular: {regular_time:.2f}s for {regular_count} rows")
    print(f"   Regular is {cube_time/regular_time:.1f}x {'faster' if regular_time < cube_time else 'slower'}")
    
    print("\n[INSIGHTS] Key Insights:")
    print("   - Cube generates ALL possible combinations (often unnecessary)")
    print("   - Regular operations only compute what you need")
    print("   - Better performance with targeted aggregations")
    print("   - More predictable memory usage")

def example_5_business_specific_aggregations(spark: SparkSession) -> None:
    """Example 5: Business-specific aggregations instead of generic cube"""
    print("\n=== EXAMPLE 5: Business-Specific Aggregations ===")
    
    df = create_sample_sales_data(spark)
    
    print("Instead of generic CUBE, create business-meaningful aggregations:")
    
    # Business Report 1: Regional Performance Dashboard
    print("\n1. Regional Performance Dashboard:")
    regional_dashboard = df.groupBy("region").agg(
        sum("revenue").alias("total_revenue"),
        count("*").alias("total_sales"),
        countDistinct("product").alias("products_sold"),
        avg("price").alias("avg_price"),
        max("price").alias("highest_sale"),
        min("price").alias("lowest_sale")
    ).withColumn("avg_sale_value", spark_round(col("total_revenue") / col("total_sales"), 2)) \
     .orderBy(desc("total_revenue"))
    
    regional_dashboard.show(truncate=False)
    
    # Business Report 2: Category Performance Analysis
    print("\n2. Category Performance Analysis:")
    category_performance = df.groupBy("category").agg(
        sum("revenue").alias("total_revenue"),
        sum("quantity").alias("total_units_sold"),
        count("*").alias("number_of_transactions"),
        countDistinct("region").alias("regions_sold_in")
    ).withColumn("revenue_per_unit", spark_round(col("total_revenue") / col("total_units_sold"), 2)) \
     .withColumn("avg_transaction_size", spark_round(col("total_revenue") / col("number_of_transactions"), 2)) \
     .orderBy(desc("total_revenue"))
    
    category_performance.show(truncate=False)
    
    # Business Report 3: Top Performers by Region
    print("\n3. Top Performing Products by Region:")
    
    # Window function for ranking within each region
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy("region").orderBy(desc("product_revenue"))
    
    top_products = df.groupBy("region", "product").agg(
        sum("revenue").alias("product_revenue"),
        count("*").alias("sales_count")
    ).withColumn("rank", rank().over(window_spec)) \
     .filter(col("rank") <= 3) \
     .orderBy("region", "rank")
    
    top_products.show(20, truncate=False)
    
    # Business Report 4: Monthly Trends
    print("\n4. Monthly Revenue Trends:")
    monthly_trends = df.withColumn("month", month(col("sale_date"))) \
                       .groupBy("month").agg(
        sum("revenue").alias("monthly_revenue"),
        avg("price").alias("avg_price"),
        count("*").alias("sales_count")
    ).withColumn("revenue_growth", 
                lag(col("monthly_revenue")).over(Window.orderBy("month"))) \
     .withColumn("growth_rate", 
                spark_round((col("monthly_revenue") - col("revenue_growth")) / col("revenue_growth") * 100, 2)) \
     .orderBy("month")
    
    monthly_trends.show(12, truncate=False)
    
    print("\n[BUSINESS] Business-Focused Approach Benefits:")
    print("   - Each report answers specific business questions")
    print("   - Results are actionable and meaningful")
    print("   - Performance optimized for actual use cases")
    print("   - Easy to add business logic and constraints")
    print("   - Better integration with BI tools")
    print("   - Clearer data lineage and documentation")

def main() -> None:
    """Run all cube to regular table examples"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Cube vs Regular Table Operations Examples")
    print("=" * 50)
    
    try:
        example_1_cube_vs_regular_aggregation(spark)
        example_2_rollup_vs_regular_hierarchy(spark)
        example_3_multidimensional_regular_analysis(spark)
        example_4_performance_comparison(spark)
        example_5_business_specific_aggregations(spark)
        
        print("\n" + "=" * 50)
        print("[SUMMARY] Summary: Cube vs Regular Tables")
        print("\n[GUIDE] When to Use Regular Tables Instead of Cube:")
        print("   - When you need specific business aggregations")
        print("   - When performance is critical")
        print("   - When results need to be easily understood")
        print("   - When integrating with BI tools")
        print("   - When you want predictable query patterns")
        
        print("\n[MIGRATION] Migration Strategy:")
        print("   1. Identify actual business requirements")
        print("   2. Replace cube with targeted groupBy operations")
        print("   3. Create separate aggregations for each use case")  
        print("   4. Optimize each query independently")
        print("   5. Add business logic and validation")
        
    except Exception as e:
        print(f"[ERROR] Error in examples: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()