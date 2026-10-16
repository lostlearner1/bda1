#!/bin/bash
# ==========================================================================
# Hadoop Setup and Execution Script
# ==========================================================================
# This script demonstrates the complete workflow:
#   1. Hadoop Environment Setup
#   2. HDFS Directory Creation and Data Upload
#   3. MapReduce Compilation and Execution
#   4. Hive Table Creation and Queries
#   5. Spark Job Submission
# ==========================================================================

echo "=========================================="
echo "  WEATHER ANALYSIS — HADOOP WORKFLOW"
echo "=========================================="

# ==========================================================================
# STEP 1: VERIFY HADOOP INSTALLATION
# ==========================================================================
echo ""
echo "--- Step 1: Verifying Hadoop Installation ---"
echo ""

echo "Hadoop Version:"
hadoop version

echo ""
echo "Java Version:"
java -version

echo ""
echo "Spark Version:"
spark-submit --version 2>&1 | head -5


# ==========================================================================
# STEP 2: START HADOOP SERVICES
# ==========================================================================
echo ""
echo "--- Step 2: Starting Hadoop Services ---"
echo ""

# Start HDFS (NameNode + DataNode)
start-dfs.sh

# Start YARN (ResourceManager + NodeManager)
start-yarn.sh

# Verify all services are running
echo ""
echo "Running Java processes:"
jps


# ==========================================================================
# STEP 3: PREPARE HDFS DIRECTORIES AND UPLOAD DATA
# ==========================================================================
echo ""
echo "--- Step 3: Setting Up HDFS ---"
echo ""

# Create input directory in HDFS
hdfs dfs -mkdir -p /user/weather

# Upload the weather dataset to HDFS
hdfs dfs -put -f weather_data.csv /user/weather/

# Verify the upload
echo "Files in /user/weather:"
hdfs dfs -ls /user/weather/

echo ""
echo "First 5 lines of uploaded data:"
hdfs dfs -cat /user/weather/weather_data.csv | head -5


# ==========================================================================
# STEP 4: COMPILE AND RUN MAPREDUCE JOB
# ==========================================================================
echo ""
echo "--- Step 4: Compiling MapReduce Program ---"
echo ""

# Set Hadoop classpath
export HADOOP_CLASSPATH=$(hadoop classpath)

# Compile the Java MapReduce program
javac -classpath $HADOOP_CLASSPATH -d . WeatherAnalysis.java

# Create the JAR file
jar -cvf WeatherAnalysis.jar *.class

echo ""
echo "--- Running MapReduce Job ---"
echo ""

# Remove any previous output directory
hdfs dfs -rm -r -f /user/weather/mapreduce_output

# Submit the MapReduce job
hadoop jar WeatherAnalysis.jar WeatherAnalysis \
    /user/weather/weather_data.csv \
    /user/weather/mapreduce_output

echo ""
echo "--- MapReduce Output ---"
echo ""
hdfs dfs -cat /user/weather/mapreduce_output/part-r-00000


# ==========================================================================
# STEP 5: RUN HIVE QUERIES
# ==========================================================================
echo ""
echo "--- Step 5: Running Hive Queries ---"
echo ""

# Execute the Hive script
hive -f hive_weather_analysis.hql


# ==========================================================================
# STEP 6: RUN SPARK JOB
# ==========================================================================
echo ""
echo "--- Step 6: Running Spark Analysis ---"
echo ""

# Submit PySpark job
spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 2 \
    --executor-memory 1g \
    --executor-cores 1 \
    spark_weather_analysis.py


# ==========================================================================
# STEP 7: VIEW RESULTS
# ==========================================================================
echo ""
echo "=========================================="
echo "  ALL ANALYSES COMPLETE — RESULTS"
echo "=========================================="

echo ""
echo "--- MapReduce Output ---"
hdfs dfs -cat /user/weather/mapreduce_output/part-r-00000

echo ""
echo "--- Spark Output Files ---"
hdfs dfs -ls /user/weather/output/

echo ""
echo "--- Seasonal Stats (Spark Output) ---"
hdfs dfs -cat /user/weather/output/seasonal_stats/*.csv

echo ""
echo "--- Extreme Events Summary (Spark Output) ---"
hdfs dfs -cat /user/weather/output/extreme_events/*.csv

echo ""
echo "--- Regional Comparison (Spark Output) ---"
hdfs dfs -cat /user/weather/output/regional_comparison/*.csv

echo ""
echo "=========================================="
echo "  WORKFLOW COMPLETED SUCCESSFULLY"
echo "=========================================="
