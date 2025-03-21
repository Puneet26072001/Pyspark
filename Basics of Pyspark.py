# Databricks notebook source
# Owner- Puneet Sharma
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

dbutils.fs.ls('/FileStore/')

# COMMAND ----------

df=spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

df_json=spark.read.format('json')\
    .option('inferSchema',True)\
    .option('multiLine',False)\
    .option('header',True)\
    .load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_ddl_schema='''
                Item_Identifier String,
                Item_Weight String,
                Item_Fat_Content String,
                Item_Visibility Double,
                Item_Type String,
                Item_MRP Double,
                Outlet_Identifier String,
                Outlet_Establishment_Year Integer,
                Outlet_Size String,
                Outlet_Location_Type String,
                Outlet_Type String,
                Item_Outlet_Sales Double
                '''
                                

# COMMAND ----------

df=spark.read.format('csv').schema(my_ddl_schema).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema=StructType([
    StructField('Item_Identifier',StringType(),True),
    StructField('Item_Weight',StringType(),True),
    StructField('Item_Fat_Content',StringType(),True),
    StructField('Item_Visibility',StringType(),True),
    StructField('Item_Type',StringType(),True),
    StructField('Item_MRP',StringType(),True),
    StructField('Outlet_Identifier',StringType(),True),
    StructField('Outlet_Establishment_Year',StringType(),True),
    StructField('Outlet_Size',StringType(),True),
    StructField('Outlet_Location_Type',StringType(),True),
    StructField('Outlet_Type',StringType(),True),
    StructField('Item_Outlet_Sales',StringType(),True),
]
)

# COMMAND ----------

df=spark.read.format('csv').schema(my_struct_schema).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Typecasting

# COMMAND ----------

df.withColumn('Item_Weight',col('Item_Weight').cast(StringType())).display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort/OrderBy

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending=[0,0]).display()

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LIMIT

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP

# COMMAND ----------

df.drop('Item_Visibility').display()

# COMMAND ----------

df.drop('Item_Visibility','Item_MRP').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Duplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.distinct().display()
