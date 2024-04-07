-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Exploratory Data Analysis
-- MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this part we will: </br>
-- MAGIC
-- MAGIC * Create tables
-- MAGIC * Create temporary views
-- MAGIC * Write basic SQL queries to explore, manipulate, and present data
-- MAGIC * Join two views and visualize the result

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###### Working with Retail Data
-- MAGIC
-- MAGIC we'll be working with the file found `dbfs:/databricks-datasets/online_retail/data-001`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Ex 1:  Create a table
-- MAGIC Create a new table named `outdoorProducts` with the following schema: 
-- MAGIC
-- MAGIC |Column Name | Type |
-- MAGIC | ---------- | ---- |
-- MAGIC |invoiceNo   | STRING |
-- MAGIC |stockCode   | STRING |
-- MAGIC |description | STRING |
-- MAGIC |quantity    | INT |
-- MAGIC |invoiceDate | STRING|
-- MAGIC |unitPrice  | DOUBLE | 
-- MAGIC |customerID  | INT |
-- MAGIC |countryName | STRING|
-- MAGIC
-- MAGIC Steps to complete: 
-- MAGIC * Make sure this notebook is idempotent by dropping any tables that have the name `outdoorProducts`
-- MAGIC * Use `csv` as the specified data source
-- MAGIC * Use the path provided above to access the data
-- MAGIC * This data contains a header; include that in your table creation statement

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###### Listing files

-- COMMAND ----------

-- MAGIC %fs ls 

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/databricks-datasets/online_retail/data-001

-- COMMAND ----------

DROP TABLE IF EXISTS outdoorProducts;
CREATE TABLE outdoorProducts (
  invoiceNo STRING,
  stockCode STRING,
  description STRING,
  quantity INT,
  invoiceDate STRING,
  unitPrice DOUBLE,
  customerID STRING,
  countryName STRING
) USING csv OPTIONS (
  path "dbfs:/databricks-datasets/online_retail/data-001/data.csv",
  header "true"
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##### Ex 2: Explore the data
-- MAGIC
-- MAGIC * Count the number of items that have a negative `quantity`
-- MAGIC This table keeps track of online transactions, including returns. Some of the quantities in the `quantity` column show a negative number. Run a query that counts then number of negative values in the `quantity` column. 
-- MAGIC * Write a query that reports the number of values less than 0 in the `quantity` column
-- MAGIC

-- COMMAND ----------

SELECT count(*)
FROM outdoorProducts
WHERE quantity < 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Ex 3: Create a temporary view
-- MAGIC * Create a temporary view that includes only the specified columns and rows, and uses math to create a new column. 
-- MAGIC * Create a temporary view named `sales`
-- MAGIC * Create a new column, `totalAmount`, by multiplying `quantity` times `unitPrice` and rounding to the nearest cent
-- MAGIC * Include columns: `stockCode`, `quantity`, `unitPrice`, `totalAmount`, `countryName`
-- MAGIC * Include only rows where `quantity` is greater than 0

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW sales AS
SELECT
  stockCode,
  quantity,
  unitPrice,
  ROUND(quantity * unitPrice, 2) AS totalAmount,
  countryName
FROM outdoorProducts
WHERE quantity > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Ex 4: Display ordered view
-- MAGIC * Show the view you created with `totalAmount` sorted greatest to least
-- MAGIC * Select all columns form the view `sales`
-- MAGIC * Order the `totalAmount` column from greatest to least
-- MAGIC * **Report the `countryName` from the row with the greatest `totalAmount` of sales **

-- COMMAND ----------

SELECT * FROM sales
ORDER BY totalAmount DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Ex 5: View countries
-- MAGIC * Show a list of all unique `countryName` values in the `sales` view
-- MAGIC * Write a query that returns only distinct `countryName` values

-- COMMAND ----------

SELECT DISTINCT(countryName)
FROM sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Ex 6: Create a temporary view: `salesQuants`
-- MAGIC * Create a temporary view that shows total `quantity` of items purchased from each `countryName`
-- MAGIC * Create a temporary view named `salesQuants`
-- MAGIC * Display the sum of all `quantity` values grouped by `countryName`. Name that column `totalQuantity`
-- MAGIC * Order the view by `totalQuantity` from greatest to least

-- COMMAND ----------

-- Create the temp view
CREATE OR REPLACE TEMP VIEW salesQuants AS
SELECT
  SUM(quantity) AS totalQuantity, countryName
FROM sales
GROUP BY countryName
ORDER BY totalQuantity DESC;

SELECT * FROM salesQuants;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Ex 7: Read in a new parquet table
-- MAGIC
-- MAGIC * Create a new table named `countryCodes`
-- MAGIC * Drop any existing tables named `countryCodes` from your database
-- MAGIC * Use this path: `/mnt/training/countries/ISOCountryCodes/ISOCountryLookup.parquet` to create a new table using parquet as the data source. Name it `countryCodes`
-- MAGIC * Include options to indicate that there **is** a header for this table

-- COMMAND ----------

DROP TABLE IF EXISTS countryCodes;
CREATE TABLE countryCodes USING parquet OPTIONS (
   path "/mnt/training/countries/ISOCountryCodes/ISOCountryLookup.parquet",
  header "true"
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Ex 8: View metadata
-- MAGIC * View column names and data types in this table.
-- MAGIC * Use the `DESCRIBE` command to display all of column names and their data types
-- MAGIC * Use the `DESCRIBE EXTENDED` command to display all of column names and their data types
-- MAGIC Returns additional metadata such as parent schema, owner, access time etc
-- MAGIC * The command `EXTENDED` is similar to `FORMATTED` 

-- COMMAND ----------

DESCRIBE EXTENDED countryCodes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Ex 9: Join and Visualize
-- MAGIC * Use the `salesQuants` view and the `countryCodes` table to display a pie chart that shows total sales by country, and identifies the country by its 3-letter id. 
-- MAGIC * Write a query that results in two columns: `totalQuantity` from `salesQuants` and `alpha3Code` from `countryCodes`
-- MAGIC * Join `countryCodes` with `salesQuants` on the name of country listed in each table
-- MAGIC * Visualize your results as a pie chart that shows the percent of sales from each country

-- COMMAND ----------

SELECT totalQuantity, countryCodes.alpha3Code AS countryAbbr
FROM salesQuants 
JOIN countryCodes 
ON countryCodes.EnglishShortName = salesQuants.CountryName

-- COMMAND ----------

SELECT DISTINCT(EnglishShortName) 
FROM countryCodes 
ORDER BY EnglishShortName DESC;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW modCountryCodes AS
SELECT
  alpha3code,
  REPLACE (
    EnglishShortName,
    "United Kingdom of Great Britain and Northern Ireland",
    "United Kingdom"
  ) AS EnglishShortName
FROM
  countryCodes;

-- COMMAND ----------

SELECT
  totalQuantity,
  modCountryCodes.alpha3Code AS countryAbbr
FROM salesQuants
JOIN modCountryCodes 
ON modCountryCodes.EnglishShortName = salesQuants.CountryName

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.stop()
