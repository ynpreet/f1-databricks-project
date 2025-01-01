# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS teamb2.bronze.constructors;
# DROP TABLE IF EXISTS teamb2.bronze.drivers;
# DROP TABLE IF EXISTS teamb2.bronze.pit_stops;
# DROP TABLE IF EXISTS teamb2.bronze.circuits;
# DROP TABLE IF EXISTS teamb2.bronze.races;
# DROP TABLE IF EXISTS teamb2.bronze.lap_times;
# DROP TABLE IF EXISTS teamb2.bronze.qualifying;
# DROP TABLE IF EXISTS teamb2.bronze.results;

# COMMAND ----------

create_bronze_table('teamb2','bronze','constructors')
create_bronze_table('teamb2','bronze','drivers')
create_bronze_table('teamb2','bronze','pit_stops')
create_bronze_table('teamb2','bronze','circuits')
create_bronze_table('teamb2','bronze','races')
create_bronze_table('teamb2','bronze','lap_times')
create_bronze_table('teamb2','bronze','qualifying')
create_bronze_table('teamb2','bronze','results')

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA bronze
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

# MAGIC %sql
# MAGIC select * from logging.log_table;

# COMMAND ----------


