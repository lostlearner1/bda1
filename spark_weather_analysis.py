# ==========================================================================
# PySpark Weather Data Analysis Program
# ==========================================================================
# This program performs comprehensive weather analysis using Apache Spark:
#   1. Seasonal Pattern Analysis
#   2. Extreme Weather Event Detection
#   3. Regional Variation Comparison
#   4. Correlation Analysis Among Weather Parameters
# ==========================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, max, min, sum, count, round as spark_round,
    col, when, corr, stddev, desc, asc, lit, concat
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# ==========================================================================
# STEP 1: Initialize SparkSession
# ==========================================================================
try:
    spark = SparkSession.builder \
        .appName("Weather Data Analysis") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()
except Exception:
    spark = SparkSession.builder \
        .appName("Weather Data Analysis") \
        .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("  WEATHER DATA ANALYSIS USING APACHE SPARK")
print("=" * 70)

# ==========================================================================
# STEP 2: Define Schema and Load Data
# ==========================================================================
schema = StructType([
    StructField("Date",           StringType(),  True),
    StructField("Location",       StringType(),  True),
    StructField("Temperature_C",  DoubleType(),  True),
    StructField("Rainfall_mm",    DoubleType(),  True),
    StructField("Humidity_pct",   DoubleType(),  True),
    StructField("WindSpeed_kmh",  DoubleType(),  True),
    StructField("Season",         StringType(),  True),
])

# Determine dataset path (local or HDFS)
import os
csv_path = "hdfs:///user/weather/weather_data.csv"
if not os.path.exists("/user/weather/weather_data.csv") and os.path.exists("weather_data.csv"):
    csv_path = "weather_data.csv"

# Load data
df = spark.read.csv(
    csv_path,
    header=True,
    schema=schema
)

# Cache the DataFrame for repeated use
df.cache()

print(f"\n[INFO] Total records loaded: {df.count()}")
print(f"[INFO] Distinct locations: {df.select('Location').distinct().count()}")
print(f"[INFO] Distinct seasons:   {df.select('Season').distinct().count()}")

print("\n--- Sample Data ---")
df.show(10, truncate=False)

print("\n--- Schema ---")
df.printSchema()


# ==========================================================================
# ANALYSIS 1: SEASONAL PATTERN ANALYSIS
# ==========================================================================
print("\n" + "=" * 70)
print("  ANALYSIS 1: SEASONAL PATTERNS")
print("=" * 70)

# 1a. Overall Seasonal Statistics
print("\n--- 1a. Overall Seasonal Weather Statistics ---")
seasonal_stats = df.groupBy("Season").agg(
    spark_round(avg("Temperature_C"), 2).alias("Avg_Temp_C"),
    spark_round(max("Temperature_C"), 2).alias("Max_Temp_C"),
    spark_round(min("Temperature_C"), 2).alias("Min_Temp_C"),
    spark_round(sum("Rainfall_mm"), 2).alias("Total_Rainfall_mm"),
    spark_round(avg("Rainfall_mm"), 2).alias("Avg_Rainfall_mm"),
    spark_round(avg("Humidity_pct"), 2).alias("Avg_Humidity"),
    spark_round(avg("WindSpeed_kmh"), 2).alias("Avg_WindSpeed"),
    count("*").alias("Record_Count")
).orderBy(
    when(col("Season") == "Winter",  1)
    .when(col("Season") == "Summer",  2)
    .when(col("Season") == "Monsoon", 3)
    .when(col("Season") == "Autumn",  4)
)
seasonal_stats.show(truncate=False)

# 1b. Seasonal Patterns Per Location
print("\n--- 1b. Seasonal Patterns Per Location ---")
location_season = df.groupBy("Location", "Season").agg(
    spark_round(avg("Temperature_C"), 2).alias("Avg_Temp"),
    spark_round(max("Temperature_C"), 2).alias("Max_Temp"),
    spark_round(min("Temperature_C"), 2).alias("Min_Temp"),
    spark_round(sum("Rainfall_mm"), 2).alias("Total_Rain"),
    spark_round(avg("Humidity_pct"), 2).alias("Avg_Humidity"),
    spark_round(avg("WindSpeed_kmh"), 2).alias("Avg_Wind")
).orderBy("Location", 
    when(col("Season") == "Winter",  1)
    .when(col("Season") == "Summer",  2)
    .when(col("Season") == "Monsoon", 3)
    .when(col("Season") == "Autumn",  4)
)
location_season.show(50, truncate=False)


# ==========================================================================
# ANALYSIS 2: EXTREME WEATHER EVENT DETECTION
# ==========================================================================
print("\n" + "=" * 70)
print("  ANALYSIS 2: EXTREME WEATHER EVENTS")
print("=" * 70)

# Define thresholds for extreme weather
EXTREME_HEAT_THRESHOLD  = 40.0   # °C
HEAVY_RAIN_THRESHOLD    = 200.0  # mm
HIGH_WIND_THRESHOLD     = 25.0   # km/h
HIGH_HUMIDITY_THRESHOLD = 90.0   # %

# 2a. Extreme Heat Events (Temp > 40°C)
print(f"\n--- 2a. Extreme Heat Events (> {EXTREME_HEAT_THRESHOLD}°C) ---")
extreme_heat = df.filter(col("Temperature_C") > EXTREME_HEAT_THRESHOLD) \
    .select("Date", "Location", "Temperature_C", "Season") \
    .orderBy(desc("Temperature_C"))
extreme_heat.show(truncate=False)
print(f"   Total extreme heat events: {extreme_heat.count()}")

# 2b. Heavy Rainfall Events (Rain > 200 mm)
print(f"\n--- 2b. Heavy Rainfall Events (> {HEAVY_RAIN_THRESHOLD} mm) ---")
heavy_rain = df.filter(col("Rainfall_mm") > HEAVY_RAIN_THRESHOLD) \
    .select("Date", "Location", "Rainfall_mm", "Season") \
    .orderBy(desc("Rainfall_mm"))
heavy_rain.show(truncate=False)
print(f"   Total heavy rainfall events: {heavy_rain.count()}")

# 2c. High Wind Events (Wind > 25 km/h)
print(f"\n--- 2c. High Wind Events (> {HIGH_WIND_THRESHOLD} km/h) ---")
high_wind = df.filter(col("WindSpeed_kmh") > HIGH_WIND_THRESHOLD) \
    .select("Date", "Location", "WindSpeed_kmh", "Season") \
    .orderBy(desc("WindSpeed_kmh"))
high_wind.show(truncate=False)
print(f"   Total high wind events: {high_wind.count()}")

# 2d. Combined Extreme Weather Summary by Location
print("\n--- 2d. Extreme Weather Summary by Location ---")
extreme_summary = df.groupBy("Location").agg(
    sum(when(col("Temperature_C") > EXTREME_HEAT_THRESHOLD, 1).otherwise(0))
        .alias("Extreme_Heat_Days"),
    sum(when(col("Rainfall_mm") > HEAVY_RAIN_THRESHOLD, 1).otherwise(0))
        .alias("Heavy_Rain_Days"),
    sum(when(col("WindSpeed_kmh") > HIGH_WIND_THRESHOLD, 1).otherwise(0))
        .alias("High_Wind_Days"),
    sum(when(col("Humidity_pct") > HIGH_HUMIDITY_THRESHOLD, 1).otherwise(0))
        .alias("High_Humidity_Days"),
    count("*").alias("Total_Records")
).orderBy("Location")
extreme_summary.show(truncate=False)


# ==========================================================================
# ANALYSIS 3: REGIONAL VARIATION ANALYSIS
# ==========================================================================
print("\n" + "=" * 70)
print("  ANALYSIS 3: REGIONAL VARIATIONS")
print("=" * 70)

# 3a. Overall Regional Comparison
print("\n--- 3a. Overall Regional Comparison ---")
regional = df.groupBy("Location").agg(
    spark_round(avg("Temperature_C"), 2).alias("Avg_Temp"),
    spark_round(max("Temperature_C"), 2).alias("Max_Temp"),
    spark_round(min("Temperature_C"), 2).alias("Min_Temp"),
    spark_round(max("Temperature_C") - min("Temperature_C"), 2).alias("Temp_Range"),
    spark_round(sum("Rainfall_mm"), 2).alias("Total_Rainfall"),
    spark_round(avg("Humidity_pct"), 2).alias("Avg_Humidity"),
    spark_round(stddev("Temperature_C"), 2).alias("Temp_StdDev"),
    count("*").alias("Records")
).orderBy(desc("Avg_Temp"))
regional.show(truncate=False)

# 3b. Temperature Range Analysis (shows climate variability)
print("\n--- 3b. Temperature Variability by Region ---")
temp_variability = df.groupBy("Location").agg(
    spark_round(max("Temperature_C") - min("Temperature_C"), 2).alias("Annual_Temp_Range"),
    spark_round(stddev("Temperature_C"), 2).alias("Temp_Std_Deviation"),
    spark_round(avg("Temperature_C"), 2).alias("Mean_Temp")
).orderBy(desc("Annual_Temp_Range"))
temp_variability.show(truncate=False)

# 3c. Rainfall Distribution by Region
print("\n--- 3c. Rainfall Distribution by Region ---")
rain_distribution = df.groupBy("Location").agg(
    spark_round(sum("Rainfall_mm"), 2).alias("Total_Annual_Rain"),
    spark_round(avg("Rainfall_mm"), 2).alias("Avg_Daily_Rain"),
    spark_round(max("Rainfall_mm"), 2).alias("Max_Single_Day_Rain"),
    sum(when(col("Rainfall_mm") > 0, 1).otherwise(0)).alias("Rainy_Days"),
    count("*").alias("Total_Days")
).orderBy(desc("Total_Annual_Rain"))
rain_distribution.show(truncate=False)


# ==========================================================================
# ANALYSIS 4: CORRELATION AMONG WEATHER PARAMETERS
# ==========================================================================
print("\n" + "=" * 70)
print("  ANALYSIS 4: RELATIONSHIPS AMONG WEATHER PARAMETERS")
print("=" * 70)

# 4a. Correlation Matrix
print("\n--- 4a. Correlation Matrix ---")
correlations = {
    "Temp_vs_Humidity":   df.stat.corr("Temperature_C", "Humidity_pct"),
    "Temp_vs_Rainfall":   df.stat.corr("Temperature_C", "Rainfall_mm"),
    "Temp_vs_Wind":       df.stat.corr("Temperature_C", "WindSpeed_kmh"),
    "Rainfall_vs_Humidity": df.stat.corr("Rainfall_mm", "Humidity_pct"),
    "Rainfall_vs_Wind":   df.stat.corr("Rainfall_mm", "WindSpeed_kmh"),
    "Humidity_vs_Wind":   df.stat.corr("Humidity_pct", "WindSpeed_kmh"),
}

print(f"  {'Parameter Pair':<25} {'Correlation':>12}  {'Interpretation'}")
print(f"  {'-'*25} {'-'*12}  {'-'*30}")
for pair, corr_val in correlations.items():
    if abs(corr_val) > 0.7:
        strength = "Strong"
    elif abs(corr_val) > 0.4:
        strength = "Moderate"
    else:
        strength = "Weak"
    direction = "Positive" if corr_val > 0 else "Negative"
    print(f"  {pair:<25} {corr_val:>12.4f}  {strength} {direction}")

# 4b. Temperature Buckets vs Other Parameters
print("\n--- 4b. Temperature Buckets vs Other Parameters ---")
temp_bucket_analysis = df.withColumn(
    "Temp_Bucket",
    when(col("Temperature_C") < 20, "Below 20°C")
    .when(col("Temperature_C") < 30, "20-30°C")
    .when(col("Temperature_C") < 40, "30-40°C")
    .otherwise("Above 40°C")
).groupBy("Temp_Bucket").agg(
    spark_round(avg("Humidity_pct"), 2).alias("Avg_Humidity"),
    spark_round(avg("Rainfall_mm"), 2).alias("Avg_Rainfall"),
    spark_round(avg("WindSpeed_kmh"), 2).alias("Avg_Wind"),
    count("*").alias("Count")
).orderBy("Temp_Bucket")
temp_bucket_analysis.show(truncate=False)

# 4c. Rainfall Category vs Other Parameters
print("\n--- 4c. Rainfall Category vs Other Parameters ---")
rain_cat_analysis = df.withColumn(
    "Rain_Category",
    when(col("Rainfall_mm") == 0, "No Rain")
    .when(col("Rainfall_mm") <= 50, "Light")
    .when(col("Rainfall_mm") <= 150, "Moderate")
    .when(col("Rainfall_mm") <= 300, "Heavy")
    .otherwise("Very Heavy")
).groupBy("Rain_Category").agg(
    spark_round(avg("Temperature_C"), 2).alias("Avg_Temp"),
    spark_round(avg("Humidity_pct"), 2).alias("Avg_Humidity"),
    spark_round(avg("WindSpeed_kmh"), 2).alias("Avg_Wind"),
    count("*").alias("Count")
).orderBy("Rain_Category")
rain_cat_analysis.show(truncate=False)

# 4d. Per-Region Correlation
print("\n--- 4d. Temperature-Humidity Correlation by Region ---")
locations = [row.Location for row in df.select("Location").distinct().collect()]
for loc in sorted(locations):
    loc_df = df.filter(col("Location") == loc)
    temp_hum_corr  = loc_df.stat.corr("Temperature_C", "Humidity_pct")
    temp_rain_corr = loc_df.stat.corr("Temperature_C", "Rainfall_mm")
    print(f"  {loc:<12}  Temp<->Humidity: {temp_hum_corr:>8.4f}  |  Temp<->Rainfall: {temp_rain_corr:>8.4f}")


# ==========================================================================
# ANALYSIS 5: SAVE RESULTS TO HDFS
# ==========================================================================
print("\n" + "=" * 70)
print("  SAVING RESULTS TO HDFS")
print("=" * 70)

# Save seasonal stats
out_dir = "/user/weather/output" if os.path.exists("/user/weather") else "spark_output"
try:
    seasonal_stats.coalesce(1).write.mode("overwrite") \
        .csv(f"{out_dir}/seasonal_stats", header=True)
    print(f"  [SAVED] {out_dir}/seasonal_stats")

    extreme_summary.coalesce(1).write.mode("overwrite") \
        .csv(f"{out_dir}/extreme_events", header=True)
    print(f"  [SAVED] {out_dir}/extreme_events")

    regional.coalesce(1).write.mode("overwrite") \
        .csv(f"{out_dir}/regional_comparison", header=True)
    print(f"  [SAVED] {out_dir}/regional_comparison")
except Exception as e:
    print(f"  [NOTE] Results displayed on console. File save note: {e}")

print("\n" + "=" * 70)
print("  ANALYSIS COMPLETE")
print("=" * 70)

# Stop SparkSession
spark.stop()
