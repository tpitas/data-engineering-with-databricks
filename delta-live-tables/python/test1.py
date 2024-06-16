# spark.readStream.table("table_name")

file_path = "/databricks-datasets/structured-streaming/events"
checkpoint_path = "/tmp/ss-tutorial/_checkpoint"

raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(file_path)
)

raw_df.display()
