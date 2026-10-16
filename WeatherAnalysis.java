import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * =========================================================================
 * WeatherAnalysis.java — MapReduce Program for Weather Data Analysis
 * =========================================================================
 *
 * PURPOSE:
 * Analyzes a CSV weather dataset to compute:
 * 1. Average Temperature per Location per Season
 * 2. Maximum Rainfall per Location per Season
 * 3. Extreme Weather Event Detection
 *
 * INPUT FORMAT (CSV):
 * Date, Location, Temperature_C, Rainfall_mm, Humidity_pct, WindSpeed_kmh,
 * Season
 *
 * USAGE:
 * hadoop jar WeatherAnalysis.jar WeatherAnalysis <input_path> <output_path>
 *
 * =========================================================================
 */
public class WeatherAnalysis {

    // =====================================================================
    // MAPPER CLASS: WeatherMapper
    // =====================================================================
    // Reads each CSV line and emits composite keys with weather values.
    //
    // Input: (line_offset, "2023-01-05,Mumbai,24.5,0.0,62,12.3,Winter")
    // Output: ("Mumbai_Winter_TEMP", 24.5)
    // ("Mumbai_Winter_RAIN", 0.0)
    // ("Mumbai_Winter_HUMIDITY", 62.0)
    // ("Mumbai_Winter_WIND", 12.3)
    // =====================================================================
    public static class WeatherMapper
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text compositeKey = new Text();
        private DoubleWritable weatherValue = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            // Skip the CSV header row
            if (line.startsWith("Date,") || line.isEmpty()) {
                return;
            }

            // Parse CSV fields
            String[] fields = line.split(",");
            if (fields.length < 7) {
                return; // Skip malformed rows
            }

            try {
                String date = fields[0].trim();
                String location = fields[1].trim();
                double temperature = Double.parseDouble(fields[2].trim());
                double rainfall = Double.parseDouble(fields[3].trim());
                double humidity = Double.parseDouble(fields[4].trim());
                double windSpeed = Double.parseDouble(fields[5].trim());
                String season = fields[6].trim();

                // ----------------------------------------------------------
                // Emit Temperature data: Key = "Location_Season_TEMP"
                // ----------------------------------------------------------
                compositeKey.set(location + "_" + season + "_TEMP");
                weatherValue.set(temperature);
                context.write(compositeKey, weatherValue);

                // ----------------------------------------------------------
                // Emit Rainfall data: Key = "Location_Season_RAIN"
                // ----------------------------------------------------------
                compositeKey.set(location + "_" + season + "_RAIN");
                weatherValue.set(rainfall);
                context.write(compositeKey, weatherValue);

                // ----------------------------------------------------------
                // Emit Humidity data: Key = "Location_Season_HUMIDITY"
                // ----------------------------------------------------------
                compositeKey.set(location + "_" + season + "_HUMIDITY");
                weatherValue.set(humidity);
                context.write(compositeKey, weatherValue);

                // ----------------------------------------------------------
                // Emit Wind Speed data: Key = "Location_Season_WIND"
                // ----------------------------------------------------------
                compositeKey.set(location + "_" + season + "_WIND");
                weatherValue.set(windSpeed);
                context.write(compositeKey, weatherValue);

                // ----------------------------------------------------------
                // Emit Extreme Weather flags
                // Extreme Heat: Temperature > 40°C
                // Extreme Rain: Rainfall > 300 mm
                // Extreme Wind: Wind Speed > 30 km/h
                // ----------------------------------------------------------
                if (temperature > 40.0) {
                    compositeKey.set("EXTREME_HEAT_" + location);
                    weatherValue.set(temperature);
                    context.write(compositeKey, weatherValue);
                }

                if (rainfall > 300.0) {
                    compositeKey.set("EXTREME_RAIN_" + location);
                    weatherValue.set(rainfall);
                    context.write(compositeKey, weatherValue);
                }

                if (windSpeed > 30.0) {
                    compositeKey.set("EXTREME_WIND_" + location);
                    weatherValue.set(windSpeed);
                    context.write(compositeKey, weatherValue);
                }

            } catch (NumberFormatException e) {
                // Skip rows with unparseable numeric values
                System.err.println("Skipping malformed row: " + line);
            }
        }
    }

    // =====================================================================
    // REDUCER CLASS: WeatherReducer
    // =====================================================================
    // For each composite key, computes:
    // - TEMP keys → Average temperature
    // - RAIN keys → Total and maximum rainfall
    // - HUMIDITY keys → Average humidity
    // - WIND keys → Average wind speed
    // - EXTREME_* → Count of extreme events + max value
    // =====================================================================
    public static class WeatherReducer
            extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            String keyStr = key.toString();

            double sum = 0.0;
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            int count = 0;

            for (DoubleWritable val : values) {
                double v = val.get();
                sum += v;
                count++;
                if (v > max)
                    max = v;
                if (v < min)
                    min = v;
            }

            double avg = sum / count;
            String result;

            // Format output based on the type of weather parameter
            if (keyStr.contains("_TEMP")) {
                result = String.format("Avg=%.2f°C | Max=%.2f°C | Min=%.2f°C | Records=%d",
                        avg, max, min, count);
            } else if (keyStr.contains("_RAIN")) {
                result = String.format("Total=%.2fmm | Avg=%.2fmm | Max=%.2fmm | Records=%d",
                        sum, avg, max, count);
            } else if (keyStr.contains("_HUMIDITY")) {
                result = String.format("Avg=%.2f%% | Max=%.2f%% | Min=%.2f%% | Records=%d",
                        avg, max, min, count);
            } else if (keyStr.contains("_WIND")) {
                result = String.format("Avg=%.2fkm/h | Max=%.2fkm/h | Records=%d",
                        avg, max, count);
            } else if (keyStr.startsWith("EXTREME_")) {
                result = String.format("ExtremeEvents=%d | MaxValue=%.2f",
                        count, max);
            } else {
                result = String.format("Sum=%.2f | Avg=%.2f | Count=%d",
                        sum, avg, count);
            }

            context.write(key, new Text(result));
        }
    }

    // =====================================================================
    // DRIVER / MAIN METHOD
    // =====================================================================
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: WeatherAnalysis <input_path> <output_path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weather Data Analysis — MapReduce");

        // Set the JAR class
        job.setJarByClass(WeatherAnalysis.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);

        // Set Mapper output key-value types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Set final output key-value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths from command-line arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Execute the job and exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
