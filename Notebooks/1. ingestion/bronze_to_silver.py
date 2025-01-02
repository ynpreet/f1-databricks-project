# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# COMMAND ----------

# dbutils.widgets.text("p_data_source", "")
# v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS teamb2.silver.constructors;
# MAGIC DROP TABLE IF EXISTS teamb2.silver.drivers;
# MAGIC DROP TABLE IF EXISTS teamb2.silver.pit_stops;
# MAGIC DROP TABLE IF EXISTS teamb2.silver.circuits;
# MAGIC DROP TABLE IF EXISTS teamb2.silver.races;
# MAGIC DROP TABLE IF EXISTS teamb2.silver.lap_times;
# MAGIC DROP TABLE IF EXISTS teamb2.silver.qualifying;
# MAGIC DROP TABLE IF EXISTS teamb2.silver.results;

# COMMAND ----------

create_silver_table('teamb2','silver','constructors')
create_silver_table('teamb2','silver','drivers')
create_silver_table('teamb2','silver','pit_stops')
create_silver_table('teamb2','silver','circuits')
create_silver_table('teamb2','silver','races')
create_silver_table('teamb2','silver','lap_times')
create_silver_table('teamb2','silver','qualifying')
create_silver_table('teamb2','silver','results')


# COMMAND ----------

# %sql
# select constructor_id,  count(*) from silver.constructors GROUP BY constructor_id HAVING count(*)>1

# COMMAND ----------

# primary_keys = {
#     'circuits': ['circuit_id'],
#     'constructors': ['constructor_id'],
#     'drivers': ['driver_id'],
#     'races': ['race_id'],
#     'results': ['result_id'],
#     'lap_times': ['race_id', 'driver_id', 'lap'],
#     'pit_stops': ['race_id', 'driver_id', 'stop'],
#     'qualifying': ['qualify_id']
# }
# schema = "silver"
# table = "constructors"
# primary_key = primary_keys[table]
# merge_condition = ' AND '.join([f"target.{col} = source.{col}" for col in primary_key])
# print(merge_condition)


# spark.sql(f"""MERGE INTO {schema}.{table} AS target
#     USING temp_view AS source
#     ON {merge_condition}
#     WHEN MATCHED THEN
#         UPDATE SET *
#     WHEN NOT MATCHED THEN
#         INSERT *;""")

# display(spark.sql(f"select count(*) from {schema}.{table}"))

# COMMAND ----------

# %sql
# select * from bronze.pit_stops

# COMMAND ----------

# %sql
# select * from silver.pit_stops target
# left join temp_view source
# ON target.race_id = source.race_id AND target.driver_id = source.driver_id AND target.stop = source.stop
# WHERE target.race_id IS NULL


# COMMAND ----------

# schema="bronze"
# file_path = f"abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/{table}.json"
# schema_table = "raceId INT, driverId INT, stop INT, lap INT, time STRING, duration STRING, milliseconds INT"
# df = spark.read.format("json").schema(schema_table).option("multiLine", True).option("header", "true").load(file_path)
# display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA silver;
# MAGIC SELECT 'constructors' AS table_name, COUNT(*) AS count FROM constructors UNION ALL
# MAGIC SELECT 'drivers', COUNT(*) FROM drivers UNION ALL
# MAGIC SELECT 'pit_stops', COUNT(*) FROM pit_stops UNION ALL
# MAGIC SELECT 'circuits', COUNT(*) FROM circuits UNION ALL
# MAGIC SELECT 'races', COUNT(*) FROM races UNION ALL
# MAGIC SELECT 'lap_times', COUNT(*) FROM lap_times UNION ALL
# MAGIC SELECT 'qualifying', COUNT(*) FROM qualifying UNION ALL
# MAGIC SELECT 'results', COUNT(*) FROM results;
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


