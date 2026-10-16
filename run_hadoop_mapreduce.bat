@echo off
echo ========================================================
echo   Running Hadoop MapReduce via Docker Container
echo ========================================================
echo.

rem Step 1: Remove old output folder if it exists
if exist mr_output (
    echo Cleaning up previous mr_output folder...
    rmdir /s /q mr_output
)

rem Step 2: Compile WeatherAnalysis.java targeting Java 8
echo [1/3] Compiling WeatherAnalysis.java...
javac --release 8 -classpath "hadoop_libs\hadoop-common-3.3.6.jar;hadoop_libs\hadoop-mapreduce-client-core-3.3.6.jar" -d . WeatherAnalysis.java

rem Step 3: Package into JAR
echo [2/3] Creating WeatherAnalysis.jar...
jar -cvf WeatherAnalysis.jar WeatherAnalysis*.class

rem Step 4: Run inside official Apache Hadoop Docker container
echo [3/3] Submitting MapReduce Job to Apache Hadoop 3...
docker run --rm -v "%cd%:/workspace" -w /workspace apache/hadoop:3 hadoop jar WeatherAnalysis.jar WeatherAnalysis weather_data.csv ./mr_output

echo.
echo ========================================================
echo   HADOOP MAPREDUCE OUTPUT (part-r-00000)
echo ========================================================
echo.
type mr_output\part-r-00000 | more

echo.
echo ========================================================
echo   Job Completed Successfully!
echo ========================================================
pause
