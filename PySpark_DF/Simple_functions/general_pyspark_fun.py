import os

import pyspark
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("General_Functions_App").getOrCreate()

# To get the absolute path.
actual_path = os.path.normpath(os.getcwd() + os.sep + os.pardir + os.sep + os.pardir)

"""
Schema:
root
 |-- id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- sal: integer (nullable = true)
 |-- dept: string (nullable = true)
"""
schema = StructType([
                     StructField("id", StringType()),
                     StructField("name", StringType()),
                     StructField("sal", IntegerType()),
                     StructField("dept", StringType())])
df = spark.read.format("csv").load(actual_path + "/input_data/emp.csv", schema=schema ,header= "true")
rename_dict = {"sum(sal)": "sal_SUM", "max(sal)":"sal_MAX", "min(sal)":"sal_MIN", "count(sal)":"sal_COUNT"}

# Filter
"""
+---+-----+-----+----+
|id |name |sal  |dept|
+---+-----+-----+----+
|1  |John |10000|IT  |
|3  |Alex |15000|IT  |
|4  |Diego|20000|IT  |
+---+-----+-----+----+
"""
df = df.filter(f.col("sal") >= 10000)
df.show(truncate=False)

# withColumn
"""
+---+-----+-----+----+------+
| id| name|  sal|dept| bonus|
+---+-----+-----+----+------+
|  1| John|10000|  IT|2000.0|
|  3| Alex|15000|  IT|3000.0|
|  4|Diego|20000|  IT|4000.0|
+---+-----+-----+----+------+
"""
df = df.withColumn("bonus", f.col("sal") * 0.2)
df.show()

# GroupBy, withColumnsRenamed
"""
+---+-------+-------+-------+---------+
|id |sal_SUM|sal_MAX|sal_MIN|sal_COUNT|
+---+-------+-------+-------+---------+
|3  |15000  |15000  |15000  |1        |
|1  |10000  |10000  |10000  |1        |
|4  |20000  |20000  |20000  |1        |
+---+-------+-------+-------+---------+
"""
df = df.groupBy("id").agg(f.sum("sal"), f.max("sal"), f.min("sal"), f.count("sal"))
rename_df = df.withColumnsRenamed(rename_dict)
rename_df.show(truncate=False)