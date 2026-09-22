# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f8300747-1fcc-478d-9c5f-c821c6c07669",
# META       "default_lakehouse_name": "LifestyleDataLocal",
# META       "default_lakehouse_workspace_id": "aad96fb1-b672-4e7f-a33b-b5180bb59cb2",
# META       "known_lakehouses": [
# META         {
# META           "id": "f8300747-1fcc-478d-9c5f-c821c6c07669"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

table_names = [
    "customers",
    "lifestyle_profiles",
    "wellness_observations",
    "activities",
    "products",
    "purchases",
    "purchase_items"
]

for table_name in table_names:
    source = spark.table(f"ontology_demo.{table_name}")
    source.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"dbo.{table_name}")
    source_count = source.count()
    target_count = spark.table(f"dbo.{table_name}").count()
    assert source_count == target_count, f"Count mismatch for {table_name}"

print("ONTOLOGY_DBO_TABLES_READY", table_names)

