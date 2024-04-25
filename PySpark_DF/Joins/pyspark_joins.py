from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("SparkApp").master("local[1]").getOrCreate()

"""
Input Data
+----+
|  id|
+----+
|   1|
|   1|
|   1|
|   2|
|NULL|
+----+

+----+
|  id|
+----+
|   1|
|   1|
|   2|
|NULL|
+----+
"""

mylist1 = [1, 1, 1, 2, None]
l = map(lambda x: Row(x), mylist1)
df1 = spark.createDataFrame(l, ["id"])
df1.show()

mylist2 = [1, 1, 2, None]
l = map(lambda x: Row(x), mylist2)
df2 = spark.createDataFrame(l, ["id"])
df2.show()

### Inner Join  ###
"""
+---+
|id |
+---+
|1  |
|1  |
|1  |
|1  |
|1  |
|1  |
|2  |
+---+
"""
inner_join_df = df1.join(df2, ["id"], "inner")
inner_join_df.show(truncate=False)


### left join ###
"""
+----+
|id  |
+----+
|NULL|
|1   |
|1   |
|1   |
|1   |
|1   |
|1   |
|2   |
+----+
"""
left_join_df = df1.join(df2, ["id"], "left")
left_join_df.show(truncate=False)

# Right Join #
"""
+----+
|id  |
+----+
|NULL|
|1   |
|1   |
|1   |
|1   |
|1   |
|1   |
|2   |
+----+
"""
right_join_df = df1.join(df2, ["id"], "right")
right_join_df.show(truncate=False)

# Full Join #
"""
+----+
|NULL|
|NULL|
|1   |
|1   |
|1   |
|1   |
|1   |
|1   |
|2   |
+----+
"""
full_join_df = df1.join(df2, ["id"], "full")
full_join_df.show(truncate=False)
