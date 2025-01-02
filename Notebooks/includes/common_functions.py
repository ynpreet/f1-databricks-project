# Databricks notebook source
#### Addinng ingestion_date timestamp to a dataframe


from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
  output_df = input_df.withColumn("ingestion_date", from_utc_timestamp(current_timestamp(), 'US/Eastern'))
  return output_df

# COMMAND ----------

#### Creating a Bronze table

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
            schema_table= "circuitId INT, circuitRef STRING, name STRING, location STRING, country STRING, lat FLOAT, lng FLOAT, alt INT, url STRING"
            df = spark.read.format("csv").schema(schema_table).option("header", "true").load(file_path)

        elif table in ['races']:
            file_path = f"abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/{table}.csv"
            schema_table = "raceId INT, year INT, round INT, circuitId INT, name STRING, date DATE, time STRING, url STRING, fp1_date DATE, fp1_time STRING, fp2_date DATE, fp2_time STRING, fp3_date DATE, fp3_time STRING, quali_date DATE, quali_time STRING, sprint_date DATE, sprint_time STRING"
            df = spark.read.format("csv").schema(schema_table).option("header", "true").load(file_path)

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
            df = spark.read.format("json").schema(schema_table).option("multiLine", True).option("header", "true").load(file_path)

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
            # Deleting non delta tables if any before creating external tables
            try:
                dbutils.fs.rm(f"abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/delta_table/{table}/", True)
                print(f"Cleaned up non-Delta files for table: {table}")
            except Exception as cleanup_error:
                print(f"Failed to clean up non-Delta files for {table}: {str(cleanup_error)}")

            spark.sql(f"""CREATE TABLE {table} 
                      USING DELTA
                      LOCATION 'abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/delta_table/{table}/' 
                        AS SELECT * FROM temp_view;""")
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

#### Creating a Silver table function

def create_silver_table(catalog, schema, table):
    try:
        # Check Connection and Container
        try:
            dbutils.fs.ls(f"abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/")
        except Exception:
            print(f"Connection or container issue detected for schema: {schema}")
            # Create External Location if it doesn't exist
            spark.sql(f"""CREATE EXTERNAL LOCATION IF NOT EXISTS {schema}_location
                        URL 'abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/'
                        WITH (STORAGE CREDENTIAL `teamb2adlsgen2`);""")
            print(f"External location created for schema: {schema}")

        # Create Catalog and Schema
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog} ")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} ")
        spark.sql(f"USE CATALOG {catalog} ")
        spark.sql(f"USE SCHEMA {schema} ")

        primary_keys = {
            'circuits': ['circuit_id'],
            'constructors': ['constructor_id'],
            'drivers': ['driver_id'],
            'races': ['race_id'],
            'results': ['result_id'],
            'lap_times': ['race_id', 'driver_id', 'lap'],
            'pit_stops': ['race_id', 'driver_id', 'stop'],
            'qualifying': ['qualify_id']
        }

        # Load Bronze Table
        bronze_table = f"bronze.{table}"
        if not spark.catalog.tableExists(bronze_table):
            raise Exception(f"Bronze table {bronze_table} does not exist.")

        # Process based on table
        if table == 'circuits':
            df = spark.sql(f"SELECT circuitId AS circuit_id, circuitRef AS circuit_ref, name, location, country, lat AS latitude, lng AS longitude, alt AS altitude FROM {bronze_table}")

        elif table == 'races':
            df=spark.sql(f"select * from {bronze_table}")
            # df = df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))
            df = spark.sql(f"SELECT raceId AS race_id, year as race_year, round, circuitId AS circuit_id, name, TO_TIMESTAMP(CONCAT(date, ' ', time), 'yyyy-MM-dd HH:mm:ss') AS race_timestamp FROM {bronze_table}")
            # df.partitionBy('race_year')

        elif table == 'constructors':
            df = spark.sql(f"SELECT constructorId AS constructor_id, constructorRef AS constructor_ref, name AS constructor_name, nationality FROM {bronze_table}")

        elif table == 'drivers':
            df = spark.sql(f"SELECT driverId AS driver_id, driverRef AS driver_ref, number, code, concat(forename,' ',surname) as name, dob, nationality FROM {bronze_table}")

        elif table == 'results':
            df = spark.sql(f"SELECT resultId AS result_id, raceId AS race_id, driverId AS driver_id, constructorId AS constructor_id, number, grid, position, positionText AS position_text, positionOrder AS position_order, points, laps, time, milliseconds, fastestLap AS fastest_lap, rank, fastestLapTime AS fastest_lap_time, fastestLapSpeed AS fastest_lap_speed FROM {bronze_table}")
            # df.partitionBy('race_id')

        elif table == 'pit_stops':
            df = spark.sql(f"SELECT raceId AS race_id, driverId AS driver_id, stop, lap, time, duration, milliseconds FROM {bronze_table}")

        elif table == 'lap_times':
            df = spark.sql(f"SELECT raceId AS race_id, driverId AS driver_id, lap, position, time, milliseconds FROM {bronze_table}")

        elif table == 'qualifying':
            df = spark.sql(f"SELECT qualifyId AS qualify_id, raceId AS race_id, driverId AS driver_id, constructorId AS constructor_id, number, position, q1, q2, q3 FROM {bronze_table}")

        else:
            raise Exception(f"Table {table} not supported for processing.")

        # Add Metadata Columns
        df = df.dropDuplicates(primary_keys[table])
        df = df.withColumn("data_source", lit("bronze"))
        df = add_ingestion_date(df)
        df.createOrReplaceTempView("temp_view")

        primary_key = primary_keys[table]
        merge_condition = ' AND '.join([f"target.{col} = source.{col}" for col in primary_key])
        print(merge_condition)

        # Get Record Count
        record_count = spark.sql(f"SELECT COUNT(*) FROM temp_view").collect()[0][0]
        if spark.catalog.tableExists(f"{schema}.{table}"):
            # spark.sql(f"INSERT INTO TABLE {table} SELECT * FROM temp_view;")
            # print(f"{record_count} records inserted in Table {catalog}.{schema}.{table}.")
            # operation = "INSERT"
            incremental_count = spark.sql(f"""
                                            SELECT COUNT(*)
                                            FROM temp_view AS source
                                            LEFT JOIN {schema}.{table} AS target
                                            ON {merge_condition}
                                            WHERE target.{primary_key[0]} IS NULL
                                        """).collect()[0][0]
            merge_sql = f"""
                MERGE INTO {schema}.{table} AS target
                USING temp_view AS source
                ON {merge_condition}
                WHEN MATCHED THEN
                  UPDATE SET *
                WHEN NOT MATCHED THEN
                  INSERT *;
            """
            spark.sql(merge_sql)
            # print(f"Incremental data merged into Table {catalog}.{schema}.{table}.")
            print(f"Incremental data merged into Table {catalog}.{schema}.{table}. Total new records: {incremental_count}")
            operation = "MERGE"
        else:
            try:
                dbutils.fs.rm(f"abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/{table}", True)
                print(f"Cleaned up non-Delta files for table: {table}")
            except Exception as cleanup_error:
                print(f"Failed to clean up non-Delta files for {table}: {str(cleanup_error)}")

            # Create Table
            spark.sql(f"""CREATE TABLE {table} 
                          USING DELTA 
                          LOCATION 'abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/{table}/' 
                          AS SELECT * FROM temp_view;""")
            print(f"New Table {catalog}.{schema}.{table} created with {record_count} records.")
            operation = "CREATE"

            # spark.sql(f"""CREATE TABLE {table} 
            #           USING DELTA 
            #           LOCATION 'abfss://{schema}@adlsgen2teamb2dev1.dfs.core.windows.net/{table}/' 
            #           AS SELECT * FROM temp_view;""")
            # print(f"New Table {catalog}.{schema}.{table} created with {record_count} records.")
            # operation = "CREATE"

        # Log table creation activity
        spark.sql("CREATE SCHEMA IF NOT EXISTS logging")
        spark.sql("USE SCHEMA logging")
        spark.sql("CREATE TABLE IF NOT EXISTS log_table (timestamp STRING, catalog STRING, schema STRING, table STRING, source STRING, status STRING, record_count INT)")
        spark.sql(f"INSERT INTO log_table VALUES (current_timestamp(), '{catalog}', '{schema}', '{table}', 'bronze', '{operation}', {record_count})")

    except Exception as e:
        print(f"Error processing table {catalog}.{schema}.{table}: {str(e)}")
        spark.sql("CREATE SCHEMA IF NOT EXISTS logging")
        spark.sql("USE SCHEMA logging")
        spark.sql("CREATE TABLE IF NOT EXISTS log_table (timestamp STRING, catalog STRING, schema STRING, table STRING, source STRING, status STRING, record_count INT)")
        spark.sql(f"INSERT INTO log_table VALUES (current_timestamp(), '{catalog}', '{schema}', '{table}', 'bronze', 'FAILED', 0)")


# COMMAND ----------


