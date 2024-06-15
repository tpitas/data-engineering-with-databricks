# Databricks notebook source
# MAGIC %md
# MAGIC ##### Load data from Postgres to Delta Lake
# MAGIC This notebook shows you how to import data from JDBC Postgres databases into a Delta Lake table using Python.

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

database_host = "<+++++++++++++>.us-east-1.rds.amazonaws.com"  # Endpoint
database_port = "5432"   # port number
database_name = "mytree_db" 
table = "inventory.products" 
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

# MAGIC %sql
# MAGIC show databases;
# MAGIC create database if not exists inventory;

# COMMAND ----------

target_table_name = "inventory.products"
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

# MAGIC %md Two tables categories and products have been created

# COMMAND ----------

# MAGIC %sql
# MAGIC -- categories table
# MAGIC select * from inventory.categories;
# MAGIC -- products table
# MAGIC select * from inventory.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Statistical information about products pricing
# MAGIC select category_id, product_name, size, price,
# MAGIC 	max(price) over(w),
# MAGIC 	min(price) over(w),
# MAGIC 	avg(price) over(w),
# MAGIC 	count(*) over(w)
# MAGIC from inventory.products
# MAGIC window w as (partition by category_id, size)
# MAGIC order by category_id, product_name, size;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row numbers
# MAGIC select sku, product_name, size,
# MAGIC 	row_number() over (partition by product_name order by sku)
# MAGIC from inventory.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dealing with case statements
# MAGIC select sku, product_name, category_id,
# MAGIC 	case
# MAGIC 		when category_id = 1 then 'Olive Oils'
# MAGIC 		when category_id = 2 then 'Flavor Infused Oils'
# MAGIC 		when category_id = 3 then 'Bath and Beauty'
# MAGIC 		when category_id = 4 then 'Wild Madagascar Vanilla'
# MAGIC 		else 'category unknown'
# MAGIC 	end as category_desc,
# MAGIC 	size, price
# MAGIC from inventory.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge Columns with coealesce
# MAGIC select * from inventory.categories;
# MAGIC -- Insert a new row
# MAGIC insert into inventory.categories values (5, null, 'Gift Baskets');
# MAGIC
# MAGIC select category_id,
# MAGIC 	coalesce(category_description, product_line) as description, product_line
# MAGIC from inventory.categories;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Convert values to null with nullif
# MAGIC select sku, product_name, category_id, nullif(size, 32) as size, price
# MAGIC from inventory.products;
