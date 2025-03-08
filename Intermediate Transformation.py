# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union By

# COMMAND ----------

data1=[(1,'Puneet'),(2,'Shivam')]
schema1= 'Rno INT,Name STRING'
df1=spark.createDataFrame(data1,schema1)

# COMMAND ----------

data2=[(3,'Jyotika'),(4,'Manya')]
schema2= 'Rno INT,Name STRING'
df2=spark.createDataFrame(data1,schema1)

# COMMAND ----------

df_union=df1.union(df2)
df_union.display()

# COMMAND ----------

data2_shuffle=[('Jyotika',3),('Manya',4)]
schema2_shuffle= 'Name STRING,Rno INT'
df2_shuffle=spark.createDataFrame(data2_shuffle,schema2_shuffle)
df2_shuffle.display()

# COMMAND ----------

df_new=df1.union(df2_shuffle)
df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union By

# COMMAND ----------

df2_new=df1.unionByName(df2_shuffle)
df2_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STRING FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC #### INITCAP

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select(initcap('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lower

# COMMAND ----------

df.select(lower('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upper

# COMMAND ----------

df.select(upper('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC #### CURRENT_DATE()

# COMMAND ----------

df=df.withColumn('Present_Date',current_date())
df.display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE_ADD()

# COMMAND ----------

df=df.withColumn('Future_Date',date_add(col('Present_Date'),7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE_SUB()

# COMMAND ----------

df=df.withColumn('Past_Date',date_sub('Present_Date',7))
df.display()

# COMMAND ----------

df=df.withColumn('Past_Date',date_add('Present_Date',-7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DateDiff()

# COMMAND ----------

df=df.withColumn('Date_Difference',datediff('Future_Date','Past_Date'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE_FORMAT()

# COMMAND ----------

df=df.withColumn('format_change',date_format('Future_Date','dd-MM-yyyy'))
df.display()
