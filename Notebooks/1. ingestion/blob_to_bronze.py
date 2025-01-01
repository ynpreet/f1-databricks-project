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

def create_bronze_table(catalog, schema, table):
    # Connection and container check before main try block
    try:
        dbutils.fs.ls(f"abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/")
    except Exception:
        print(f"Connection or container issue detected for schema: {schema}")
        return  # Exit the function gracefully

    try:
        # Create Catalog and Schema
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog} ")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} ")
        spark.sql(f"USE CATALOG {catalog} ")
        spark.sql(f"USE SCHEMA {schema} ")
        data_source = v_data_source

        # File Path and Schema Mapping
        if table in ['circuits']:
            file_path = f"abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/{table}.csv"
            df = spark.read.format("csv").option("header", "true").load(file_path)

        elif table in ['races']:
            file_path = f"abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/{table}.csv"
            schema_table = "raceId INT, year INT, round INT, circuitId INT, name STRING, date DATE, time STRING, url STRING, fp1_date DATE, fp1_time STRING, fp2_date DATE, fp2_time STRING, fp3_date DATE, fp3_time STRING, quali_date DATE, quali_time STRING, sprint_date DATE, sprint_time STRING"
            df = spark.read.format("csv").schema(schema_table).load(file_path)

        elif table in ['constructors']:
            file_path = f"abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/{table}.json"
            schema_table = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"
            df = spark.read.format("json").schema(schema_table).option("header", "true").load(file_path)

        elif table in ['drivers']:
            file_path = f"abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/{table}.json"
            schema_table = "driverId INT, driverRef STRING, number INT, code STRING, forename STRING, surname STRING, dob DATE, nationality STRING, url STRING"
            df = spark.read.format("json").schema(schema_table).option("header", "true").load(file_path)

        elif table in ['pit_stops']:
            file_path = f"abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/{table}.json"
            schema_table = "raceId INT, driverId INT, stop INT, lap INT, time STRING, duration STRING, milliseconds INT"
            df = spark.read.format("json").schema(schema_table).option("header", "true").load(file_path)

        elif table in ['results']:
            file_path = f"abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/{table}.json"
            schema_table = "resultId INT, raceId INT, driverId INT, constructorId INT, number INT, grid INT, position INT, positionText STRING, positionOrder INT, points FLOAT, laps INT, time STRING, milliseconds INT, fastestLap INT, rank INT, fastestLapTime STRING, fastestLapSpeed STRING, statusId INT"
            df = spark.read.format("json").schema(schema_table).option("header", "true").load(file_path)

        elif table in ['lap_times']:
            file_path = f"abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/{table}/"
            schema_table = "raceId INT, driverId INT, lap INT, position INT, time STRING, milliseconds INT"
            df = spark.read.format("csv").schema(schema_table).load(file_path)

        elif table in ['qualifying']:
            file_path = f"abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/{table}/"
            schema_table = "qualifyId INT, raceId INT, driverId INT, constructorId INT, number INT, position INT, q1 STRING, q2 STRING, q3 STRING"
            df = spark.read.format("json").schema(schema_table).option("multiLine", True).load(file_path)

        else:
            raise Exception(f"Unable to locate {table} in {v_data_source}")


        # Processing Data
        final_df = add_ingestion_date(df)
        final_df.withColumn("data_source", lit(v_data_source))
        final_df.createOrReplaceTempView("temp_view")

        # Get Record Count
        record_count = spark.sql(f"SELECT COUNT(*) FROM temp_view").collect()[0][0]
        if spark.catalog.tableExists(f"{schema}.{table}") == True:
            spark.sql(f"INSERT INTO TABLE {table}  SELECT * FROM temp_view;")
            print(f"{record_count} records inserted in Table {catalog}.{schema}.{table}.")
            operation = "INSERT"
        else:
            spark.sql(f"CREATE TABLE {table} USING DELTA AS SELECT * FROM temp_view;")
            print(f"New Table {catalog}.{schema}.{table} created with {record_count} records.")
            operation = "CREATE"

        # Log table creation activity
        spark.sql("CREATE SCHEMA IF NOT EXISTS logging")
        spark.sql("USE SCHEMA logging")
        spark.sql("CREATE TABLE IF NOT EXISTS log_table (timestamp STRING, catalog STRING, schema STRING, table STRING, source STRING, status STRING, record_count INT)")
        spark.sql(f"INSERT INTO log_table VALUES (current_timestamp(), '{catalog}', '{schema}', '{table}', '{data_source}', '{operation}', {record_count})")

    except Exception as e:
        print(f"Error processing table {catalog}.{schema}.{table}: {str(e)}")
        spark.sql("CREATE SCHEMA IF NOT EXISTS logging")
        spark.sql("USE SCHEMA logging")
        spark.sql("CREATE TABLE IF NOT EXISTS log_table (timestamp STRING, catalog STRING, schema STRING, table STRING, source STRING, status STRING, record_count INT)")
        spark.sql(f"INSERT INTO log_table VALUES (current_timestamp(), '{catalog}', '{schema}', '{table}', '{data_source}', 'FAILED', 0)")


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


