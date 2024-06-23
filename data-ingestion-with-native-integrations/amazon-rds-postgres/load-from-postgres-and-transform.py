# Databricks notebook source
# MAGIC %md
# MAGIC ##### Load data from Postgres to Delta Lake
# MAGIC This notebook shows you how to import data from JDBC Postgres databases into a Delta Lake table using Python.
# MAGIC Created a PostgreSQL RDS Database with AWS follow: 
# MAGIC https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_Tutorials.WebServerDB.CreateDBInstance.html
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###### Step 1: Connection information
# MAGIC
# MAGIC First define some variables to programmatically create these connections.
# MAGIC
# MAGIC Replace all the variables in angle brackets `<>` below with the corresponding information.

# COMMAND ----------

driver = "org.postgresql.Driver"

database_host = "<++++++++++++++++>.us-east-1.rds.amazonaws.com"   # Endpoint aws 
database_port = "5432" 
database_name = "shelter" 
table = "animals" 
user = "postgres"
password = "+++++++++++++++"

url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"

print(url)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The full URL printed out above should look something like:
# MAGIC
# MAGIC ```
# MAGIC jdbc:postgresql://localhost:5432/my_database
# MAGIC ```
# MAGIC
# MAGIC ##### Check connectivity
# MAGIC
# MAGIC Depending on security settings for your Postgres database and Databricks workspace, you may not have the proper ports open to connect.
# MAGIC
# MAGIC Replace `<database-host-url>` with the universal locator for your Postgres implementation. If you are using a non-default port, also update the 5432.
# MAGIC
# MAGIC Run the cell below to confirm Databricks can reach your Postgres database.

# COMMAND ----------

# MAGIC %sh
# MAGIC nc -vz "<database-host-url>" 5432

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Reading the data
# MAGIC
# MAGIC Now that you've specified the file metadata, you can create a DataFrame. Use an *option* to infer the data schema from the file. You can also explicitly set this to a particular schema if you have one already.
# MAGIC
# MAGIC First, create a DataFrame in Python, referencing the variables defined above.

# COMMAND ----------

remote_table = (spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("dbtable", table)
    .option("user", user)
    .option("password", password)
    .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC You can view the results of this remote table query.

# COMMAND ----------

display(remote_table)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3: Create a Delta table
# MAGIC
# MAGIC The DataFrame defined and displayed above is a temporary connection to the remote database.
# MAGIC
# MAGIC To ensure that this data can be accessed by relevant users throughout your workspace, save it as a Delta Lake table using the code below.

# COMMAND ----------

target_table_name = "animals"
remote_table.write.mode("overwrite").saveAsTable(target_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC This table will persist across cluster sessions, notebooks, and personas throughout your organization.
# MAGIC
# MAGIC The code below demonstrates querying this data with Python and SQL.

# COMMAND ----------

display(spark.table(target_table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4: Dealing with queries

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 	an1.species, an1.name,an1.admission_date,an1.primary_color,
# MAGIC 		(	SELECT 	COUNT (*) 
# MAGIC 			FROM 	animals AS an2
# MAGIC 			WHERE an2.species = an1.species
# MAGIC 		) AS number_of_species
# MAGIC FROM 	animals AS an1
# MAGIC ORDER BY an1.species, an1.admission_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PARTITION BY species
# MAGIC SELECT 	species,
# MAGIC 		name, admission_date,
# MAGIC 		primary_color, COUNT (*) 
# MAGIC 		OVER (PARTITION BY species) AS number_of_species_animals
# MAGIC FROM 	animals
# MAGIC ORDER BY 	species, admission_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimized subquery solution
# MAGIC SELECT  an.species, an.name, an.admission_date,
# MAGIC             an.primary_color, species_counts.species_animals
# MAGIC FROM    animals AS an
# MAGIC         INNER JOIN 
# MAGIC         (   SELECT  species, COUNT(*) AS species_animals
# MAGIC             FROM    animals
# MAGIC             GROUP BY species
# MAGIC         ) AS species_counts
# MAGIC         ON an.species = species_counts.species
# MAGIC ORDER BY    an.species , an.admission_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use of CTEs to find out a date 
# MAGIC WITH filtered_animals AS
# MAGIC ( 	SELECT 	* 
# MAGIC 		FROM 	animals
# MAGIC 		WHERE species = 'Cat' AND admission_date > '2019-04-24'
# MAGIC )
# MAGIC SELECT 	fa1.species, fa1.name, 
# MAGIC 		fa1.primary_color, fa1.admission_date,
# MAGIC 		(	SELECT 	COUNT (*) 
# MAGIC 			FROM 	filtered_animals AS fa2
# MAGIC 			WHERE fa2.species = fa1.species AND
# MAGIC 					  fa2.admission_date < fa1.admission_date
# MAGIC 		) AS day_species_animals
# MAGIC FROM 	filtered_animals AS fa1
# MAGIC ORDER BY 	fa1.species, fa1.admission_date;
