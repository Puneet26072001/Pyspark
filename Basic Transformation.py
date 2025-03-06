# Databricks notebook source
# MAGIC %md
# MAGIC Data Loading

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT 

# COMMAND ----------

df.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()

# COMMAND ----------

df.select(col('Item_Identifier'),col('Item_Weight')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

df.select('Item_Identifier').alias('Item_Id').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILTER

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCENARIO 1

# COMMAND ----------

df.filter(col('Item_Fat_Content')=="Regular").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCENARIO 2

# COMMAND ----------

df.filter((col('Item_Type')=="Soft Drinks") & (col('Item_Weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WithColumnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### With Column

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 1

# COMMAND ----------

df=df.withColumn('NewlyAdded',lit("New"))
df.display()

# COMMAND ----------

df.withColumn('multiply',col('Item_MRP')*col('Item_Outlet_Sales')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2

# COMMAND ----------

df.withColumn('Item_Fat_Count',regexp_replace(col('Item_Fat_Content'),'Regular','Reg'))\
    .withColumn('Item_Fat_Count',regexp_replace(col('Item_Fat_Content'),'Low Fat','LF')).display()

# COMMAND ----------


