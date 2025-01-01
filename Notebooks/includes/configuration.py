# Databricks notebook source


# COMMAND ----------

bronze_storage_account_name = "adlsgen2mdftest"
bronze_container_name = "control-tables"
bronze_folder_path = "metadata"
bronze_file_name = "bronze_layer_control_table.csv"

# Metadata Table Path
bronze_control_table_path = f"abfss://{bronze_container_name}@{bronze_storage_account_name}.dfs.core.windows.net/{bronze_folder_path}/{bronze_file_name}"

# COMMAND ----------

silver_storage_account_name = "adlsgen2mdftest"
silver_container_name = "control-tables"
silver_folder_path = "metadata"
silver_file_name = "silver_layer_control_table.csv"
silver_control_table_path = f"abfss://{silver_container_name}@{silver_storage_account_name}.dfs.core.windows.net/{silver_folder_path}/{silver_file_name}"

# COMMAND ----------

#importing all libraries
from pyspark.sql.types import *
# from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from delta.tables import DeltaTable
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------


