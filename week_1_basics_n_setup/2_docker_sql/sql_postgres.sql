================================================================
SELECT 
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  total_amount,
  CONCAT(zpu."Borough", ' / ', zpu."Zone") AS "pickup_loc",
  CONCAT(zdo."Borough", ' / ', zdo."Zone") AS "dropoff_loc"
FROM 
  yellow_taxi_trips t,
  zones zpu,
  zones zdo
WHERE 
  t."PULocationID" = zpu."LocationID" AND
  t."DOLocationID" = zdo."LocationID"
LIMIT 100;

================================================================
-- -- The same statement as above but using JOIN (INNER JOIN)
-- SELECT
--   tpep_pickup_datetime,
--   tpep_dropoff_datetime,
--   total_amount,
--   CONCAT(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
--   CONCAT(zdo."Borough", '/', zdo."Zone") AS "dropoff_loc"
-- FROM
--   yellow_taxi_trips t JOIN zones zpu
--     ON t."PULocationID" = zpu."LocationID"
--   JOIN zones zdo
--     ON t."DOLocationID" = zdo."LocationID"
-- LIMIT 100;

================================================================
-- -- Select data from yellow_taxi_trips whose drop off / pickup 
-- -- location ID do not appear in the zones table.
-- SELECT
--     tpep_pickup_datetime,
--     tpep_dropoff_datetime,
--     total_amount,
--     "PULocationID",
--     "DOLocationID"
-- FROM
--     yellow_taxi_trips t
-- WHERE
--     "DOLocationID" NOT IN (
--         SELECT "LocationID" FROM zones
--     )
-- LIMIT 100;

================================================================================
-- -- Delete all data in the zones table with Location ID of 142
-- DELETE FROM zones WHERE "LocationID" = 142;

================================================================================
-- Using LEFT JOIN to show data of yellow_taxi_trips only
SELECT
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  total_amount,
  CONCAT(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
  CONCAT(zdo."Borough", '/', zdo."Zone") AS "dropoff_loc"
FROM
  yellow_taxi_trips t LEFT JOIN zones zpu
    ON t."PULocationID" = zpu."LocationID"
  LEFT JOIN zones zdo
    ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;

================================================================================
SELECT
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  -- 	-- Display as date with timestamp
  -- 	DATE_TRUNC('day', tpep_pickup_datetime) as day,
  -- Display as date
  CAST(tpep_pickup_datetime as DATE) as day,
  total_amount
FROM
  yellow_taxi_trips t 
LIMIT 100;

================================================================================
-- Group by
-- 1, 2 indicate the 1st & 2nd arguments after SELECT
SELECT
  CAST(tpep_pickup_datetime AS DATE) as "day",
  "DOLocationID",
  COUNT(1) as "count",
  MAX(total_amount),
  MAX(passenger_count)
FROM
  yellow_taxi_trips t
GROUP BY
  1, 2
ORDER BY 
	"day" ASC,
	"DOLocationID" ASC;