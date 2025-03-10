# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### collect_list

# COMMAND ----------

data=[('user1','Maths'),
    ('user1','English'),
    ('user2','English'),
    ('user2','SST'),
    ('user3','Maths')]
schema='User STRING,Name STRING'
df_book=spark.createDataFrame(data,schema)
df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('Name')).display()

# COMMAND ----------

df=spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivot

# COMMAND ----------

df.groupby('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### When-Otherwise

# COMMAND ----------

df_veg=df.withColumn('veg_flag',when(col('Item_Type')=='Meat','Non-Veg').otherwise('Veg'))

# COMMAND ----------

df_veg.withColumn('veg_exp_flag',when((col('veg_flag')=='Veg') & (col('Item_MRP')>100),'Veg Expensive').when((col('veg_flag')=='Veg') & (col('Item_MRP')<100),'Inexpensive').otherwise('Non-Veg')).display()
