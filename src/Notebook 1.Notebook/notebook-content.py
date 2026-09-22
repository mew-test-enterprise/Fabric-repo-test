# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d2cbed23-183a-4bfd-8344-dcaa292c0be4",
# META       "default_lakehouse_name": "LifestyleData",
# META       "default_lakehouse_workspace_id": "aad96fb1-b672-4e7f-a33b-b5180bb59cb2",
# META       "known_lakehouses": [
# META         {
# META           "id": "d2cbed23-183a-4bfd-8344-dcaa292c0be4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LifestyleData.dbo.lifestyle_data LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/Files/lifestyle_data.csv"
df = pd.read_csv("/lakehouse/default/Files/lifestyle_data.csv")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Example PySpark code to read a Delta Lake table from the Lakehouse

# Replace 'tablename' below with your actual table name from the Lakehouse.

df = spark.read.format("delta").load("/Tables/tablename")
df.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
