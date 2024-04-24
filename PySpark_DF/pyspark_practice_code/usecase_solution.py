import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkApp").master("local[1]").getOrCreate()

""" 
Problem.1 : From 1| Mumbai -> Delhi -> Srinagar to 1| Mumbai  -> Srinagar

Problem:
+---+--------+-----------+
| id| sources|       dest|
+---+--------+-----------+
|  1|Mumbai|Delhi        | 
|  1|Delhi |Srinagar     |
|  2|Chenai|Hyderabad    |
+---+--------+-----------+

Expected:
+---+--------+-----------+
| id| sources|       dest|
+---+--------+-----------+
|  1|[Mumbai]| [Srinagar]|
|  2|[Chenai]|[Hyderabad]|
+---+--------+-----------+
"""
# Prepare Data.
data = ((1, "Mumbai", "Delhi"),(1, "Delhi", "Srinagar"),(2, "Chenai", "Hyderabad"))
col = ["id", "sources", "dest"]

# Rename dictionary.
dict_col = {"collect_list(sources AS s)":"sources", "collect_list(dest AS d)":"dest"}
df = spark.createDataFrame(data, col)

# Actual logic to get the sources and target destination.
grouped_df = df.groupBy("id").agg(f.collect_list(f.col("sources").alias("s")), f.collect_list(f.col("dest").alias("d")))
rename_df = grouped_df.withColumnsRenamed(dict_col)
final_df = rename_df.select("id", f.array_except("sources", "dest").alias("sources"), f.array_except("dest", "sources").alias("dest"))
final_df.show()