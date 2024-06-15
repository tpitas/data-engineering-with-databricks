-- Databricks notebook source
-- Ingesting the dataset using Cloud Files: BRONZE datasets

CREATE STREAMING LIVE TABLE customers
COMMENT "The customers buying finished products, ingested from /databricks-datasets."
TBLPROPERTIES ("quality" = "mapping")
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/customers/", "csv");

CREATE STREAMING LIVE TABLE sales_orders_raw
COMMENT "The raw sales orders, ingested from /databricks-datasets."
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT * FROM cloud_files("/databricks-datasets/retail-org/sales_orders/", "json", map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

-- Expectations and high quality data : SILVER Datasets

CREATE STREAMING LIVE TABLE sales_orders_cleaned(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
)
PARTITIONED BY (order_date)
COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
TBLPROPERTIES ("quality" = "silver")
AS
SELECT f.customer_id, f.customer_name, f.number_of_line_items,
  TIMESTAMP(FROM_UNIXTIME((CAST(f.order_datetime AS long)))) AS order_datetime,
  DATE(FROM_UNIXTIME((CAST(f.order_datetime AS long)))) AS order_date,
  f.order_number, f.ordered_products, c.state, c.city, c.lon, c.lat, c.units_purchased, c.loyalty_segment
  FROM STREAM(LIVE.sales_orders_raw) f
  LEFT JOIN LIVE.customers c
    ON c.customer_id = f.customer_id
    AND c.customer_name = f.customer_name

-- COMMAND ----------

-- GOLD datasets

CREATE LIVE TABLE sales_order_in_dublin
COMMENT "Sales orders in DUBLIN."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr,
SUM(ordered_products_explode.price) AS sales,
SUM(ordered_products_explode.qty) AS qantity,
COUNT(ordered_products_explode.id) AS product_count
FROM (
SELECT city, DATE(order_datetime) AS order_date, customer_id, customer_name,
EXPLODE(ordered_products) AS ordered_products_explode
FROM LIVE.sales_orders_cleaned
WHERE city = 'DUBLIN'
  )
GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr;

CREATE LIVE TABLE sales_order_in_chicago
COMMENT "Sales orders in Chicago."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT city, order_date, customer_id, customer_name,
ordered_products_explode.curr,
SUM(ordered_products_explode.price) AS sales,
SUM(ordered_products_explode.qty) AS qantity,
COUNT(ordered_products_explode.id) AS product_count
FROM (
  SELECT city, DATE(order_datetime) AS order_date, customer_id, customer_name,
EXPLODE(ordered_products) AS ordered_products_explode
  FROM LIVE.sales_orders_cleaned
  WHERE city = 'Chicago'
  )
GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr;

CREATE LIVE TABLE sales_order_in_columbus
COMMENT "Sales orders in Columbus OH"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT city, order_date, customer_id, customer_name,
ordered_products_explode.curr,
SUM(ordered_products_explode.price) AS sales,
SUM(ordered_products_explode.qty) AS qantity,
COUNT(ordered_products_explode.id) AS product_count
FROM (
  SELECT city, DATE(order_datetime) AS order_date, customer_id, customer_name,
EXPLODE(ordered_products) AS ordered_products_explode
  FROM LIVE.sales_orders_cleaned
  WHERE city = 'COLUMBUS'
  )
GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr;
