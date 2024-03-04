#Ingest a new dataset from a CSV

bikes_csv_file_path = f"{datasets_dir}/nyc_tlc_yellow_trips_2018_subset_1.csv"
print(dbutils.fs.head(bikes_csv_file_path))

bikes_csv_path = f"{datasets_dir}/nyc_tlc_yellow_trips_2018_subset_1.csv"
bikes_df = (spark
               .read
               .option("header", True)
               .option("inferSchema", True)
               .csv(bikes_csv_path)
              )

# bikes_df.printSchema()
bikes_df.show()

# Create a TempView
bikes_df.createTempView("bikes")

# Select the 5 first rows
spark.sql("select * from bikes limit 5").show()

# List the top 5 most expensive trips of the year
fare_df = spark.sql("""
    select vendor_id, trip_distance, fare_amount, total_amount
    from 
      bikes
    order by
      fare_amount desc
    limit  2
""")
fare_df.show()

