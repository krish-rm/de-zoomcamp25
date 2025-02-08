-- Create an External Table in BigQuery. Once the Parquet files are uploaded to GCS, create an external table in BigQuery that references the files stored in your GCS bucket. This will allow BigQuery to read the Parquet data directly

CREATE OR REPLACE EXTERNAL TABLE `focus-heuristic-448107-a5.homework_module3.external_yellow_taxi_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3_2025_krm/gyellow_tripdata_2024-*.parquet']
);

-- Create a Native Table from the External Table (Without Partitioning or Clustering)

CREATE OR REPLACE TABLE `focus-heuristic-448107-a5.homework_module3.yellow_taxi_2024_non_partitioned`
AS
SELECT * FROM `focus-heuristic-448107-a5.homework_module3.external_yellow_taxi_2024`;


-- Question 1: What is count of records for the 2024 Yellow Taxi Data?

SELECT COUNT(*) 
FROM `focus-heuristic-448107-a5.homework_module3.yellow_taxi_2024_non_partitioned`;


-- Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs_external
FROM `focus-heuristic-448107-a5.homework_module3.external_yellow_taxi_2024`;

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs_non_partitioned
FROM `focus-heuristic-448107-a5.homework_module3.yellow_taxi_2024_non_partitioned`;


-- Question 3: Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.

SELECT DISTINCT PULocationID
FROM `focus-heuristic-448107-a5.homework_module3.yellow_taxi_2024_non_partitioned`;

SELECT DISTINCT PULocationID, DOLocationID
FROM `focus-heuristic-448107-a5.homework_module3.yellow_taxi_2024_non_partitioned`;


-- Question 4: How many records have a fare_amount of 0?

SELECT COUNT(*) AS records_with_zero_fare
FROM `focus-heuristic-448107-a5.homework_module3.yellow_taxi_2024_non_partitioned`
WHERE fare_amount = 0;


-- Question 5: What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

CREATE OR REPLACE TABLE `focus-heuristic-448107-a5.homework_module3.yellow_taxi_2024_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)  -- Partition by tpep_dropoff_datetime
CLUSTER BY VendorID  -- Cluster by VendorID
AS
SELECT *
FROM `focus-heuristic-448107-a5.homework_module3.yellow_taxi_2024_non_partitioned`;


-- Question 6: Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive) Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

SELECT DISTINCT VendorID
FROM `focus-heuristic-448107-a5.homework_module3.yellow_taxi_2024_non_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';


SELECT DISTINCT VendorID
FROM `focus-heuristic-448107-a5.homework_module3.yellow_taxi_2024_optimized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';





