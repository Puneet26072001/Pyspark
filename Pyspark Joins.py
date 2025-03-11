# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE DATAFRAMES

# COMMAND ----------

data1=[(1,'Puneet','D0'),(2,'Shivam','D1'),(3,'Jyotika','D2'),(4,'Manya','D3'),(5,'Gagan','D3'),(6,'Satyam','D5')]
schema1= 'Sno Integer,Name STRING,DeptNo STRING'
df1=spark.createDataFrame(data1,schema1)


# COMMAND ----------

data2=[('D0','HR'),('D1','IT'),('D2','SDE'),('D3','CLOUD'),('D4','SalesForce'),('D5','Manager')]
schema2= 'DeptNo STRING,Department STRING'
df2=spark.createDataFrame(data2,schema2)


# COMMAND ----------

# MAGIC %md
# MAGIC #### INNER JOIN

# COMMAND ----------

df1.join(df2,df1['DeptNo']==df2['DeptNo'],'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### LEFT JOIN

# COMMAND ----------

df1.join(df2,df1['DeptNo']==df2['DeptNo'],'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### RIGHT JOIN

# COMMAND ----------

df1.join(df2,df1['DeptNo']==df2['DeptNo'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### OUTER JOIN

# COMMAND ----------

df1.join(df2,df1['DeptNo']==df2['DeptNo'],'outer').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ANTI JOIN

# COMMAND ----------

df2.join(df1,df1['DeptNo']==df2['DeptNo'],'anti').display()
