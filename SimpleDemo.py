# Databricks notebook source
# MAGIC %sql
# MAGIC select color,sum(carat) as tcarats from lynch.diamonds group by color order by tcarats

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lynch.diamonds;

# COMMAND ----------

from pyspark.sql.types import DoubleType
from pyspark.sql.functions import sum

df = spark.read.table('lynch.diamonds')
newDf = df.withColumn("caratd", df["carat"].cast("double"))
result = newDf.groupby('color').agg(sum(newDf["caratd"]))


display(result)

# COMMAND ----------

print(df.count())