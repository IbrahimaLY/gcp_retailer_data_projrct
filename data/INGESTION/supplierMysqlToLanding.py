from google.cloud import storage, bigquery
import pandas as pd
from pyspark.sql import SparkSession
import datetime
import json

# Initialize Spark session
spark = SparkSession.builder.appName("SupplierMysqlToLanding").getOrCreate()

# Google Cloud Storage (GCS) configuration variables
GCS_BUCKET = "retailer-datalake-project-08072025"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/supplier-db/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/supplier-db/archive/"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/supplier_config.csv"

# BigQuery configuration
BQ_PROJECT = "iconic-lane-464915-i5"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

# MySQL Configuration
MYSQL_CONFIG = {
    "url": "jdbc:mysql://35.232.12.80:3306/supplierDB?useSSL=false&allowPublicKeyRetrieval=true",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "password": "mypass"
}

# Initialize GCS & BigQuery clients
storage_client = storage.Client()
bq_client = bigquery.Client()

# Logging Mechanism
log_entries = [] # Store logs before writing to GCS
##------------------------------------------------------------------------------------------##
def log_event(event_type, message, table=None):
    """Logs an event and stores it in the log list."""
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table_name": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}") # Print for visibility
##------------------------------------------------------------------------------------------##

def save_logs_to_gcs():
    """Saves the logs to a JSON file  and upload to GCS."""
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"

    json_data = json.dumps(log_entries, indent=4)


    # Get GCS bucket 
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)

    # Upload JSON data as a file
    blob.upload_from_string(json_data, content_type="application/json")

    print(f"✅ Logs successfully saved to GCS at gs://{GCS_BUCKET}/{log_filepath}")

def save_logs_to_bigquery():
    """Saves logs to BigQuery."""
    if log_entries:
        log_df = spark.createDataFrame(log_entries)
        log_df.write.format("bigquery") \
            .option("table", BQ_LOG_TABLE) \
            .option("temporaryGcsBucket", BQ_TEMP_PATH) \
            .mode("append") \
            .save()
        print(f"✅ Logs stored in BigQuery for future analysis")

##------------------------------------------------------------------------------------------##
# Function to read Config File from GCS
def read_config_file():
    df = spark.read.csv(CONFIG_FILE_PATH, header=True)
    log_event("INFO", "✅ Successfully read the config file")   
    return df

##------------------------------------------------------------------------------------------##
# Function to move existing files to archive
def move_existing_files_to_archive(table):
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/supplier-db/{table}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith("json")]

    if not existing_files:
        log_event("INFO", f"No existing files found for table {table}")
        return
    
    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)
    
        # Extract Date from file name
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]

        # Move to archive
        archive_path = f"landing/supplier-db/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)

        # Copy the file to archive and delete the original
        storage_client.bucket(GCS_BUCKET).copy_blob(source_blob, storage_client.bucket(GCS_BUCKET), destination_blob.name)
        source_blob.delete()

        log_event("INFO", f"✅ Moved {file} to {archive_path}", table=table)

##------------------------------------------------------------------------------------------##
# Function to get the latest watermark from BigQuery Audit table
def get_latest_watermark(table_name):
    query = f"""
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{BQ_AUDIT_TABLE}`
        WHERE tablename = '{table_name}'
    """
    query_job = bq_client.query(query)
    result = query_job.result()
    
    for row in result:
        return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"
    return "1900-01-01 00:00:00"

##------------------------------------------------------------------------------------------##

# Function to extract data from MySQL and save to GCS
def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        # Get Latest Watermark
        latest_watermark = get_latest_watermark(table) if load_type.lower() == "incremental" else None
        log_event("INFO", f"Latest watermark for {table}: {latest_watermark}, table=table")

        # Generate SQL Query
        query = f"(SELECT * FROM {table}) AS t" if load_type.lower() == "full load" else \
        f"(SELECT * FROM {table} WHERE {watermark_col} > '{latest_watermark}') AS t"
        
        # Read data from MySQL
        df = (spark.read
              .format("jdbc")
              .option("url", MYSQL_CONFIG["url"])
              .option("driver", MYSQL_CONFIG["driver"])
              .option("user", MYSQL_CONFIG["user"])
              .option("password", MYSQL_CONFIG["password"])
              .option("dbtable", query)
              .load())
        log_event("SUCCESS", f"✅ Successfully extracted data from {table}", table=table)

        # Convert Spark DataFrame JSON
        pandas_df = df.toPandas()
        json_data = pandas_df.to_json(orient="records", lines=True)

        # Generate File Path in GCS
        today = datetime.datetime.now().strftime("%d%m%Y")
        JSON_FILE_PATH = f"landing/supplier-db/{table}/{today}/{table}_{today}.json"

        # Upload JSON to GCS
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(JSON_FILE_PATH)
        blob.upload_from_string(json_data, content_type="application/json")

        log_event("SUCCESS", f"✅ JSON file successfully written to gs://{GCS_BUCKET}/{JSON_FILE_PATH}", table=table)

        # Insert Audit Entry
        audit_df = spark.createDataFrame([
            (table, load_type, df.count(), datetime.datetime.now(), "SUCCESS")
        ], ["tablename", "load_type", "record_count", "load_timestamp", "status"])
        
        (audit_df.write
         .format("bigquery")
         .option("table", BQ_AUDIT_TABLE)
         .option("temporaryGcsBucket", GCS_BUCKET)
         .mode("append")
         .save())

        log_event("SUCCESS", f"✅ Audit log updated for {table}", table=table)

    except Exception as e:
        log_event("ERROR", f"Error processing {table}: {str(e)}", table=table)

##------------------------------------------------------------------------------------------##
        
# Main Execution
config_df = read_config_file()

# config_df.show()

for row in config_df.collect():
    if row["is_active"] == '1':
        db, src, table, load_type, watermark, _, targetpath = row
        move_existing_files_to_archive(table)
        extract_and_save_to_landing(table, load_type, watermark)

save_logs_to_gcs()
save_logs_to_bigquery()

print("✅ Pipeline completed successfully!")