-- ==========================================================================
-- Hive Queries for Weather Data Analysis
-- ==========================================================================
-- This script creates a Hive table from the weather CSV dataset
-- and runs analytical queries for seasonal patterns, extreme events,
-- regional variations, and parameter relationships.
-- ==========================================================================


-- ==========================================================================
-- STEP 1: CREATE EXTERNAL TABLE
-- ==========================================================================
-- Load the weather CSV data into a Hive-managed table.
-- ROW FORMAT DELIMITED tells Hive that fields are comma-separated.

CREATE TABLE IF NOT EXISTS weather_data (
    record_date     STRING,
    location        STRING,
    temperature_c   DOUBLE,
    rainfall_mm     DOUBLE,
    humidity_pct    DOUBLE,
    windspeed_kmh   DOUBLE,
    season          STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Load data from HDFS into the table
LOAD DATA INPATH '/user/weather/weather_data.csv'
INTO TABLE weather_data;

-- Quick sanity check
SELECT * FROM weather_data LIMIT 10;


-- ==========================================================================
-- QUERY 1: SEASONAL PATTERNS
-- ==========================================================================
-- Compute average temperature, total rainfall, average humidity,
-- and average wind speed for each season across all locations.

SELECT
    season,
    ROUND(AVG(temperature_c), 2)    AS avg_temperature,
    ROUND(SUM(rainfall_mm), 2)      AS total_rainfall,
    ROUND(AVG(humidity_pct), 2)     AS avg_humidity,
    ROUND(AVG(windspeed_kmh), 2)    AS avg_wind_speed,
    COUNT(*)                         AS record_count
FROM weather_data
GROUP BY season
ORDER BY
    CASE season
        WHEN 'Winter'  THEN 1
        WHEN 'Summer'  THEN 2
        WHEN 'Monsoon' THEN 3
        WHEN 'Autumn'  THEN 4
    END;

-- Expected output shows clear seasonal transitions:
-- Winter  → Low temp, Low rainfall, Moderate humidity
-- Summer  → High temp, Low rainfall, Low humidity
-- Monsoon → Moderate temp, Very high rainfall, High humidity
-- Autumn  → Moderate temp, Decreasing rainfall, Moderate humidity


-- ==========================================================================
-- QUERY 2: SEASONAL PATTERNS PER LOCATION
-- ==========================================================================
-- Drill down to see how each city's weather varies by season.

SELECT
    location,
    season,
    ROUND(AVG(temperature_c), 2)    AS avg_temp,
    ROUND(MAX(temperature_c), 2)    AS max_temp,
    ROUND(MIN(temperature_c), 2)    AS min_temp,
    ROUND(SUM(rainfall_mm), 2)      AS total_rain,
    ROUND(AVG(humidity_pct), 2)     AS avg_humidity,
    ROUND(AVG(windspeed_kmh), 2)    AS avg_wind
FROM weather_data
GROUP BY location, season
ORDER BY location, 
    CASE season
        WHEN 'Winter'  THEN 1
        WHEN 'Summer'  THEN 2
        WHEN 'Monsoon' THEN 3
        WHEN 'Autumn'  THEN 4
    END;


-- ==========================================================================
-- QUERY 3: EXTREME WEATHER EVENT DETECTION
-- ==========================================================================
-- Identify days with extreme weather conditions:
--   Extreme Heat:     Temperature  > 40°C
--   Heavy Rainfall:   Rainfall     > 200 mm
--   High Wind:        Wind Speed   > 25 km/h
--   Very High Humidity: Humidity   > 90%

-- 3a. Extreme Heat Events
SELECT
    record_date,
    location,
    temperature_c,
    season
FROM weather_data
WHERE temperature_c > 40.0
ORDER BY temperature_c DESC;

-- 3b. Heavy Rainfall Events
SELECT
    record_date,
    location,
    rainfall_mm,
    season
FROM weather_data
WHERE rainfall_mm > 200.0
ORDER BY rainfall_mm DESC;

-- 3c. High Wind Events
SELECT
    record_date,
    location,
    windspeed_kmh,
    season
FROM weather_data
WHERE windspeed_kmh > 25.0
ORDER BY windspeed_kmh DESC;

-- 3d. Combined Extreme Weather Summary
SELECT
    location,
    SUM(CASE WHEN temperature_c > 40.0  THEN 1 ELSE 0 END)  AS extreme_heat_days,
    SUM(CASE WHEN rainfall_mm   > 200.0 THEN 1 ELSE 0 END)  AS heavy_rain_days,
    SUM(CASE WHEN windspeed_kmh > 25.0  THEN 1 ELSE 0 END)  AS high_wind_days,
    SUM(CASE WHEN humidity_pct  > 90.0  THEN 1 ELSE 0 END)  AS high_humidity_days
FROM weather_data
GROUP BY location
ORDER BY location;


-- ==========================================================================
-- QUERY 4: REGIONAL VARIATIONS
-- ==========================================================================
-- Compare weather parameters across different cities.

-- 4a. Overall Regional Summary
SELECT
    location,
    ROUND(AVG(temperature_c), 2)  AS avg_temp,
    ROUND(MAX(temperature_c), 2)  AS max_temp,
    ROUND(MIN(temperature_c), 2)  AS min_temp,
    ROUND(MAX(temperature_c) - MIN(temperature_c), 2)  AS temp_range,
    ROUND(SUM(rainfall_mm), 2)    AS total_rainfall,
    ROUND(AVG(humidity_pct), 2)   AS avg_humidity,
    ROUND(AVG(windspeed_kmh), 2)  AS avg_wind,
    COUNT(*)                       AS total_records
FROM weather_data
GROUP BY location
ORDER BY avg_temp DESC;

-- 4b. Hottest and Coldest City by Season
SELECT
    season,
    MAX(STRUCT(temperature_c, location)).col2 AS hottest_city,
    MAX(STRUCT(temperature_c, location)).col1 AS hottest_temp,
    MIN(STRUCT(temperature_c, location)).col2 AS coldest_city,
    MIN(STRUCT(temperature_c, location)).col1 AS coldest_temp
FROM weather_data
GROUP BY season;

-- 4c. Wettest and Driest City
SELECT
    location,
    ROUND(SUM(rainfall_mm), 2) AS total_annual_rainfall
FROM weather_data
GROUP BY location
ORDER BY total_annual_rainfall DESC;


-- ==========================================================================
-- QUERY 5: RELATIONSHIPS AMONG WEATHER PARAMETERS
-- ==========================================================================
-- Analyze correlations and relationships between parameters.

-- 5a. Temperature vs Humidity Relationship (inverse relationship expected)
SELECT
    CASE
        WHEN temperature_c < 20   THEN 'Below 20°C'
        WHEN temperature_c < 30   THEN '20-30°C'
        WHEN temperature_c < 40   THEN '30-40°C'
        ELSE 'Above 40°C'
    END AS temp_bucket,
    ROUND(AVG(humidity_pct), 2)   AS avg_humidity,
    ROUND(AVG(rainfall_mm), 2)   AS avg_rainfall,
    ROUND(AVG(windspeed_kmh), 2) AS avg_wind,
    COUNT(*)                      AS record_count
FROM weather_data
GROUP BY
    CASE
        WHEN temperature_c < 20   THEN 'Below 20°C'
        WHEN temperature_c < 30   THEN '20-30°C'
        WHEN temperature_c < 40   THEN '30-40°C'
        ELSE 'Above 40°C'
    END
ORDER BY temp_bucket;

-- 5b. Rainfall vs Humidity Relationship (positive correlation expected)
SELECT
    CASE
        WHEN rainfall_mm = 0        THEN 'No Rain'
        WHEN rainfall_mm <= 50      THEN 'Light (0-50mm)'
        WHEN rainfall_mm <= 150     THEN 'Moderate (50-150mm)'
        WHEN rainfall_mm <= 300     THEN 'Heavy (150-300mm)'
        ELSE 'Very Heavy (>300mm)'
    END AS rain_category,
    ROUND(AVG(humidity_pct), 2)   AS avg_humidity,
    ROUND(AVG(temperature_c), 2) AS avg_temp,
    ROUND(AVG(windspeed_kmh), 2) AS avg_wind,
    COUNT(*)                      AS record_count
FROM weather_data
GROUP BY
    CASE
        WHEN rainfall_mm = 0        THEN 'No Rain'
        WHEN rainfall_mm <= 50      THEN 'Light (0-50mm)'
        WHEN rainfall_mm <= 150     THEN 'Moderate (50-150mm)'
        WHEN rainfall_mm <= 300     THEN 'Heavy (150-300mm)'
        ELSE 'Very Heavy (>300mm)'
    END
ORDER BY rain_category;

-- 5c. Wind Speed vs Rainfall Relationship
SELECT
    CASE
        WHEN windspeed_kmh < 10  THEN 'Calm (<10 km/h)'
        WHEN windspeed_kmh < 15  THEN 'Light (10-15 km/h)'
        WHEN windspeed_kmh < 20  THEN 'Moderate (15-20 km/h)'
        WHEN windspeed_kmh < 30  THEN 'Strong (20-30 km/h)'
        ELSE 'Very Strong (>30 km/h)'
    END AS wind_category,
    ROUND(AVG(rainfall_mm), 2)   AS avg_rainfall,
    ROUND(AVG(humidity_pct), 2)  AS avg_humidity,
    ROUND(AVG(temperature_c), 2) AS avg_temp,
    COUNT(*)                      AS record_count
FROM weather_data
GROUP BY
    CASE
        WHEN windspeed_kmh < 10  THEN 'Calm (<10 km/h)'
        WHEN windspeed_kmh < 15  THEN 'Light (10-15 km/h)'
        WHEN windspeed_kmh < 20  THEN 'Moderate (15-20 km/h)'
        WHEN windspeed_kmh < 30  THEN 'Strong (20-30 km/h)'
        ELSE 'Very Strong (>30 km/h)'
    END
ORDER BY wind_category;


-- ==========================================================================
-- QUERY 6: MONTHLY TREND ANALYSIS
-- ==========================================================================
-- Extract month from date and see month-over-month changes.

SELECT
    SUBSTR(record_date, 6, 2) AS month,
    ROUND(AVG(temperature_c), 2)  AS avg_temp,
    ROUND(SUM(rainfall_mm), 2)    AS total_rain,
    ROUND(AVG(humidity_pct), 2)   AS avg_humidity,
    ROUND(AVG(windspeed_kmh), 2)  AS avg_wind
FROM weather_data
GROUP BY SUBSTR(record_date, 6, 2)
ORDER BY month;
