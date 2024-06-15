-- Databricks notebook source
-- Data Quality Monitoring , Visualization and Reporting
CREATE OR REPLACE TABLE my_pipeline_logs
AS SELECT * FROM delta.`dbfs:/pipelines/89c71c9e-67e6-4dab-afbb-c2a8a506d9ca/system/events`

-- COMMAND ----------

WITH all_expectations AS (
    SELECT
        explode(
          from_json(
            details:flow_progress:data_quality:expectations,
            schema_of_json("[{'name':'str', 'dataset':'str',
            'passed_records':'int', 'failed_records':'int'}]")
          )
        ) AS expectation
      FROM my_pipeline_logs
      WHERE details:flow_progress.metrics IS NOT NULL
)
SELECT expectation_name,  X_Axis, SUM(Y_Axis) AS Y_Axis
FROM (
    SELECT expectation.name AS expectation_name, 'Passed'
AS X_Axis, expectation.passed_records AS Y_Axis
    FROM all_expectations
    UNION ALL
    SELECT expectation.name AS expectation_name, 'Failed'
AS X_Axis, expectation.failed_records AS Y_Axis
    FROM all_expectations
  )
GROUP BY expectation_name, X_Axis
