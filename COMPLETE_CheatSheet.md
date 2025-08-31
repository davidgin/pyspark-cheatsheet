# PySpark Complete Cheat Sheet

## DataFrame Creation

**Basic Setup:**
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
```

**Create from Data:**
```python
# From list
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

# From file
df = spark.read.parquet("file.parquet")
df = spark.read.json("file.json") 
df = spark.read.csv("file.csv", header=True, inferSchema=True)
```

---

## Basic Operations

**View Data:**
```python
df.show()              # Show first 20 rows
df.show(5)             # Show first 5 rows
df.printSchema()       # Show schema
df.count()             # Row count
df.columns             # Column names
```

**Select Columns:**
```python
df.select("name", "age")
df.select(df.name, df.age + 1)
```

**Filter Rows:**
```python
df.filter(df.age > 21)
df.where(df.name == "Alice")
```

---

## Column Operations

**Add/Modify Columns:**
```python
df.withColumn("age_plus_one", df.age + 1)
df.withColumnRenamed("name", "full_name")
df.drop("age")
```

**Sort:**
```python
df.orderBy("age")
df.sort(df.age.desc())
```  

---

## Aggregations

**Group By:**    
```python
df.groupBy("department").count()
df.groupBy("dept").agg({"salary": "avg", "age": "max"})
```

**Common Functions:**
```python
from pyspark.sql.functions import sum, avg, max, min, count
df.select(avg("salary"), max("age"))
```

---

## SQL Functions

**Import Functions:**
```python
from pyspark.sql.functions import col, lit, when
from pyspark.sql.functions import upper, lower, length, trim
from pyspark.sql.functions import year, month, current_date
```

**String Functions:**
```python
df.select(upper(col("name")))
df.select(length(col("name")).alias("name_len"))
```

**Conditional Logic:**
```python
df.select(when(col("age") > 18, "Adult").otherwise("Minor"))
```

---

## Joins

**Basic Joins:**
```python
# Inner join (default)
df1.join(df2, "common_column")
df1.join(df2, df1.id == df2.user_id)

# Other join types
df1.join(df2, "id", "left")
df1.join(df2, "id", "right") 
df1.join(df2, "id", "outer")
```

---

## Window Functions

**Setup Window:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, lag

windowSpec = Window.partitionBy("dept").orderBy("salary")
```

**Use Window Functions:**
```python
df.withColumn("row_num", row_number().over(windowSpec))
df.withColumn("prev_salary", lag("salary", 1).over(windowSpec))
```

---

## Null Handling

**Check for Nulls:**
```python
from pyspark.sql.functions import isnull, isnan
df.select(isnull(col("name")))
```

**Handle Nulls:**
```python
# Fill nulls
df.na.fill("Unknown", ["name"])
df.na.fill(0, ["age"])

# Drop nulls
df.na.drop()
df.na.drop(subset=["name"])
```

---

## File I/O

**Read Files:**
```python
df = spark.read.parquet("input.parquet")
df = spark.read.json("input.json")
df = spark.read.csv("input.csv", header=True)
```

**Write Files:**
```python
df.write.mode("overwrite").parquet("output.parquet")
df.write.mode("append").json("output.json")
df.write.partitionBy("year").parquet("partitioned_output")
```

**Write Modes:**
- `overwrite` - Replace existing
- `append` - Add to existing  
- `ignore` - Skip if exists
- `error` - Fail if exists (default)

---

## Performance

**Partitioning:**
```python
df.repartition(4)              # 4 partitions
df.repartition("department")   # Partition by column
df.coalesce(2)                 # Reduce partitions
```

**Caching:**
```python
df.cache()        # Cache in memory
df.persist()      # Persist with default storage
df.unpersist()    # Remove from cache
```

**Broadcast Joins:**
```python
from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_df), "key")
```

---

## Streaming

**Read Stream:**
```python
streamDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "host:9092").load()
```

**Write Stream:**
```python
query = streamDF.writeStream.format("console").start()
query = streamDF.writeStream.format("parquet").option("path", "output").start()
```

**Stream Control:**
```python
query.awaitTermination()
query.stop()
query.isActive
```

---

## SQL Interface

**Register Views:**
```python
df.createOrReplaceTempView("people")
result = spark.sql("SELECT * FROM people WHERE age > 21")
```

---

## UDFs

**Create UDF:**
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def upper_case(s):
    return s.upper() if s else s

upper_udf = udf(upper_case, StringType())
df.withColumn("name_upper", upper_udf(col("name")))
```

---

## Common Patterns

**Remove Duplicates:**
```python
df.distinct()
df.dropDuplicates(["name", "email"])
```

**Sample Data:**
```python
df.sample(0.1)    # 10% sample
df.take(10)       # First 10 rows
```

**Pivot:**
```python
df.groupBy("name").pivot("department").sum("salary")
```

---

## Configuration

**Session Config:**
```python
spark = SparkSession.builder.appName("App").config("spark.sql.adaptive.enabled", "true").getOrCreate()
```

**Runtime Config:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.get("spark.sql.shuffle.partitions")
```

---

## Performance Tips

1. **Use column pruning** - Select only needed columns
2. **Push down filters** - Filter early in the pipeline  
3. **Broadcast small tables** - Use `broadcast()` for joins
4. **Partition appropriately** - Balance partition size
5. **Cache frequently used DataFrames**
6. **Use Parquet format** - Columnar storage for analytics
7. **Avoid `collect()`** - Use `take()` for small samples