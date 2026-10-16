"""
==========================================================================
Weather Data Analysis — Standalone Python Version
==========================================================================
Performs the SAME analyses as the Hadoop MapReduce / Hive / Spark programs
but runs locally without requiring Hadoop installation.

Analyses:
  1. Seasonal Pattern Analysis
  2. Extreme Weather Event Detection
  3. Regional Variation Comparison
  4. Correlation Among Weather Parameters

Usage:
  python weather_analysis_standalone.py
==========================================================================
"""

import csv
import os
from collections import defaultdict
from math import sqrt


# ==========================================================================
# STEP 1: LOAD THE DATASET
# ==========================================================================
def load_data(filepath):
    """Load weather CSV data into a list of dictionaries."""
    data = []
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append({
                'Date':          row['Date'].strip(),
                'Location':      row['Location'].strip(),
                'Temperature_C': float(row['Temperature_C'].strip()),
                'Rainfall_mm':   float(row['Rainfall_mm'].strip()),
                'Humidity_pct':  float(row['Humidity_pct'].strip()),
                'WindSpeed_kmh': float(row['WindSpeed_kmh'].strip()),
                'Season':        row['Season'].strip(),
            })
    return data


# ==========================================================================
# HELPER: Compute statistics for a list of values
# ==========================================================================
def compute_stats(values):
    """Compute count, sum, avg, min, max, stddev for a list of numbers."""
    n = len(values)
    if n == 0:
        return {'count': 0, 'sum': 0, 'avg': 0, 'min': 0, 'max': 0, 'stddev': 0}
    s = sum(values)
    avg = s / n
    mn = min(values)
    mx = max(values)
    variance = sum((x - avg) ** 2 for x in values) / n if n > 1 else 0
    stddev = sqrt(variance)
    return {'count': n, 'sum': s, 'avg': avg, 'min': mn, 'max': mx, 'stddev': stddev}


def correlation(x_vals, y_vals):
    """Compute Pearson correlation coefficient between two lists."""
    n = len(x_vals)
    if n < 2:
        return 0.0
    x_mean = sum(x_vals) / n
    y_mean = sum(y_vals) / n
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_vals, y_vals))
    denom_x = sqrt(sum((x - x_mean) ** 2 for x in x_vals))
    denom_y = sqrt(sum((y - y_mean) ** 2 for y in y_vals))
    if denom_x == 0 or denom_y == 0:
        return 0.0
    return numerator / (denom_x * denom_y)


# ==========================================================================
# ANALYSIS 1: SEASONAL PATTERNS (Simulates MapReduce + Hive Query 1 & 2)
# ==========================================================================
def seasonal_pattern_analysis(data):
    print("\n" + "=" * 70)
    print("  ANALYSIS 1: SEASONAL PATTERNS")
    print("=" * 70)

    # --- 1a. Overall Seasonal Statistics ---
    print("\n--- 1a. Overall Seasonal Weather Statistics ---\n")

    season_data = defaultdict(lambda: {'temp': [], 'rain': [], 'hum': [], 'wind': []})
    for row in data:
        s = row['Season']
        season_data[s]['temp'].append(row['Temperature_C'])
        season_data[s]['rain'].append(row['Rainfall_mm'])
        season_data[s]['hum'].append(row['Humidity_pct'])
        season_data[s]['wind'].append(row['WindSpeed_kmh'])

    header = f"{'Season':<10} {'Avg Temp':>10} {'Max Temp':>10} {'Min Temp':>10} {'Total Rain':>12} {'Avg Humid':>10} {'Avg Wind':>10} {'Records':>8}"
    print(header)
    print("-" * len(header))

    season_order = ['Winter', 'Summer', 'Monsoon', 'Autumn']
    for season in season_order:
        if season in season_data:
            d = season_data[season]
            ts = compute_stats(d['temp'])
            rs = compute_stats(d['rain'])
            hs = compute_stats(d['hum'])
            ws = compute_stats(d['wind'])
            print(f"{season:<10} {ts['avg']:>10.2f} {ts['max']:>10.2f} {ts['min']:>10.2f} "
                  f"{rs['sum']:>12.2f} {hs['avg']:>10.2f} {ws['avg']:>10.2f} {ts['count']:>8d}")

    # --- 1b. Seasonal Patterns Per Location ---
    print("\n--- 1b. Seasonal Patterns Per Location ---\n")

    loc_season = defaultdict(lambda: defaultdict(lambda: {'temp': [], 'rain': [], 'hum': [], 'wind': []}))
    for row in data:
        loc_season[row['Location']][row['Season']]['temp'].append(row['Temperature_C'])
        loc_season[row['Location']][row['Season']]['rain'].append(row['Rainfall_mm'])
        loc_season[row['Location']][row['Season']]['hum'].append(row['Humidity_pct'])
        loc_season[row['Location']][row['Season']]['wind'].append(row['WindSpeed_kmh'])

    header = f"{'Location':<12} {'Season':<10} {'Avg Temp':>10} {'Max Temp':>10} {'Min Temp':>10} {'Total Rain':>12} {'Avg Humid':>10} {'Avg Wind':>10}"
    print(header)
    print("-" * len(header))

    for loc in sorted(loc_season.keys()):
        for season in season_order:
            if season in loc_season[loc]:
                d = loc_season[loc][season]
                ts = compute_stats(d['temp'])
                rs = compute_stats(d['rain'])
                hs = compute_stats(d['hum'])
                ws = compute_stats(d['wind'])
                print(f"{loc:<12} {season:<10} {ts['avg']:>10.2f} {ts['max']:>10.2f} {ts['min']:>10.2f} "
                      f"{rs['sum']:>12.2f} {hs['avg']:>10.2f} {ws['avg']:>10.2f}")


# ==========================================================================
# ANALYSIS 2: EXTREME WEATHER EVENTS (Simulates MapReduce Extreme Detection)
# ==========================================================================
def extreme_weather_analysis(data):
    print("\n" + "=" * 70)
    print("  ANALYSIS 2: EXTREME WEATHER EVENTS")
    print("=" * 70)

    HEAT_THRESH = 40.0
    RAIN_THRESH = 200.0
    WIND_THRESH = 25.0
    HUMID_THRESH = 90.0

    # 2a. Extreme Heat
    print(f"\n--- 2a. Extreme Heat Events (Temperature > {HEAT_THRESH}°C) ---\n")
    heat_events = [r for r in data if r['Temperature_C'] > HEAT_THRESH]
    heat_events.sort(key=lambda x: x['Temperature_C'], reverse=True)

    if heat_events:
        print(f"{'Date':<14} {'Location':<12} {'Temp (°C)':>10} {'Season':<10}")
        print("-" * 50)
        for r in heat_events:
            print(f"{r['Date']:<14} {r['Location']:<12} {r['Temperature_C']:>10.1f} {r['Season']:<10}")
    print(f"\n   Total extreme heat events: {len(heat_events)}")

    # 2b. Heavy Rainfall
    print(f"\n--- 2b. Heavy Rainfall Events (Rainfall > {RAIN_THRESH} mm) ---\n")
    rain_events = [r for r in data if r['Rainfall_mm'] > RAIN_THRESH]
    rain_events.sort(key=lambda x: x['Rainfall_mm'], reverse=True)

    if rain_events:
        print(f"{'Date':<14} {'Location':<12} {'Rain (mm)':>10} {'Season':<10}")
        print("-" * 50)
        for r in rain_events:
            print(f"{r['Date']:<14} {r['Location']:<12} {r['Rainfall_mm']:>10.1f} {r['Season']:<10}")
    print(f"\n   Total heavy rainfall events: {len(rain_events)}")

    # 2c. High Wind
    print(f"\n--- 2c. High Wind Events (Wind Speed > {WIND_THRESH} km/h) ---\n")
    wind_events = [r for r in data if r['WindSpeed_kmh'] > WIND_THRESH]
    wind_events.sort(key=lambda x: x['WindSpeed_kmh'], reverse=True)

    if wind_events:
        print(f"{'Date':<14} {'Location':<12} {'Wind (km/h)':>12} {'Season':<10}")
        print("-" * 52)
        for r in wind_events:
            print(f"{r['Date']:<14} {r['Location']:<12} {r['WindSpeed_kmh']:>12.1f} {r['Season']:<10}")
    print(f"\n   Total high wind events: {len(wind_events)}")

    # 2d. Summary by Location
    print("\n--- 2d. Extreme Weather Summary by Location ---\n")
    loc_extremes = defaultdict(lambda: {'heat': 0, 'rain': 0, 'wind': 0, 'humid': 0, 'total': 0})
    for r in data:
        loc = r['Location']
        loc_extremes[loc]['total'] += 1
        if r['Temperature_C'] > HEAT_THRESH:
            loc_extremes[loc]['heat'] += 1
        if r['Rainfall_mm'] > RAIN_THRESH:
            loc_extremes[loc]['rain'] += 1
        if r['WindSpeed_kmh'] > WIND_THRESH:
            loc_extremes[loc]['wind'] += 1
        if r['Humidity_pct'] > HUMID_THRESH:
            loc_extremes[loc]['humid'] += 1

    print(f"{'Location':<12} {'Heat>40°C':>10} {'Rain>200mm':>11} {'Wind>25km/h':>12} {'Humid>90%':>10} {'Total Rec':>10}")
    print("-" * 68)
    for loc in sorted(loc_extremes.keys()):
        e = loc_extremes[loc]
        print(f"{loc:<12} {e['heat']:>10} {e['rain']:>11} {e['wind']:>12} {e['humid']:>10} {e['total']:>10}")


# ==========================================================================
# ANALYSIS 3: REGIONAL VARIATIONS (Simulates Hive Query 4)
# ==========================================================================
def regional_variation_analysis(data):
    print("\n" + "=" * 70)
    print("  ANALYSIS 3: REGIONAL VARIATIONS")
    print("=" * 70)

    # 3a. Overall Regional Comparison
    print("\n--- 3a. Overall Regional Comparison ---\n")
    loc_data = defaultdict(lambda: {'temp': [], 'rain': [], 'hum': [], 'wind': []})
    for r in data:
        loc_data[r['Location']]['temp'].append(r['Temperature_C'])
        loc_data[r['Location']]['rain'].append(r['Rainfall_mm'])
        loc_data[r['Location']]['hum'].append(r['Humidity_pct'])
        loc_data[r['Location']]['wind'].append(r['WindSpeed_kmh'])

    print(f"{'Location':<12} {'Avg Temp':>9} {'Max Temp':>9} {'Min Temp':>9} {'Range':>7} {'Total Rain':>11} {'Avg Humid':>10} {'Temp StdDev':>12}")
    print("-" * 85)
    locations_sorted = sorted(loc_data.keys(), key=lambda l: compute_stats(loc_data[l]['temp'])['avg'], reverse=True)
    for loc in locations_sorted:
        ts = compute_stats(loc_data[loc]['temp'])
        rs = compute_stats(loc_data[loc]['rain'])
        hs = compute_stats(loc_data[loc]['hum'])
        temp_range = ts['max'] - ts['min']
        print(f"{loc:<12} {ts['avg']:>9.2f} {ts['max']:>9.2f} {ts['min']:>9.2f} {temp_range:>7.2f} "
              f"{rs['sum']:>11.2f} {hs['avg']:>10.2f} {ts['stddev']:>12.2f}")

    # 3b. Rainfall Distribution
    print("\n--- 3b. Rainfall Distribution by Region ---\n")
    print(f"{'Location':<12} {'Total Rain':>11} {'Avg Rain':>9} {'Max Rain':>9} {'Rainy Days':>11} {'Total Days':>11}")
    print("-" * 67)
    for loc in sorted(loc_data.keys(), key=lambda l: compute_stats(loc_data[l]['rain'])['sum'], reverse=True):
        rs = compute_stats(loc_data[loc]['rain'])
        rainy_days = sum(1 for v in loc_data[loc]['rain'] if v > 0)
        print(f"{loc:<12} {rs['sum']:>11.2f} {rs['avg']:>9.2f} {rs['max']:>9.2f} {rainy_days:>11} {rs['count']:>11}")


# ==========================================================================
# ANALYSIS 4: RELATIONSHIPS AMONG WEATHER PARAMETERS (Simulates Spark)
# ==========================================================================
def correlation_analysis(data):
    print("\n" + "=" * 70)
    print("  ANALYSIS 4: RELATIONSHIPS AMONG WEATHER PARAMETERS")
    print("=" * 70)

    temps = [r['Temperature_C'] for r in data]
    rains = [r['Rainfall_mm'] for r in data]
    humids = [r['Humidity_pct'] for r in data]
    winds = [r['WindSpeed_kmh'] for r in data]

    # 4a. Correlation Matrix
    print("\n--- 4a. Correlation Matrix ---\n")
    pairs = [
        ("Temp vs Humidity",   temps, humids),
        ("Temp vs Rainfall",   temps, rains),
        ("Temp vs Wind",       temps, winds),
        ("Rainfall vs Humidity", rains, humids),
        ("Rainfall vs Wind",   rains, winds),
        ("Humidity vs Wind",   humids, winds),
    ]

    print(f"  {'Parameter Pair':<25} {'Correlation':>12}  {'Interpretation'}")
    print(f"  {'-'*25} {'-'*12}  {'-'*25}")
    for name, x, y in pairs:
        c = correlation(x, y)
        if abs(c) > 0.7:
            strength = "Strong"
        elif abs(c) > 0.4:
            strength = "Moderate"
        else:
            strength = "Weak"
        direction = "Positive" if c > 0 else "Negative"
        print(f"  {name:<25} {c:>12.4f}  {strength} {direction}")

    # 4b. Temperature Buckets
    print("\n--- 4b. Temperature Buckets vs Other Parameters ---\n")
    buckets = defaultdict(lambda: {'hum': [], 'rain': [], 'wind': []})
    for r in data:
        t = r['Temperature_C']
        if t < 20:
            b = "Below 20°C"
        elif t < 30:
            b = "20-30°C"
        elif t < 40:
            b = "30-40°C"
        else:
            b = "Above 40°C"
        buckets[b]['hum'].append(r['Humidity_pct'])
        buckets[b]['rain'].append(r['Rainfall_mm'])
        buckets[b]['wind'].append(r['WindSpeed_kmh'])

    print(f"  {'Temp Bucket':<15} {'Avg Humidity':>13} {'Avg Rainfall':>13} {'Avg Wind':>10} {'Count':>7}")
    print(f"  {'-'*15} {'-'*13} {'-'*13} {'-'*10} {'-'*7}")
    for bucket in ["Below 20°C", "20-30°C", "30-40°C", "Above 40°C"]:
        if bucket in buckets:
            d = buckets[bucket]
            print(f"  {bucket:<15} {compute_stats(d['hum'])['avg']:>13.2f} "
                  f"{compute_stats(d['rain'])['avg']:>13.2f} "
                  f"{compute_stats(d['wind'])['avg']:>10.2f} "
                  f"{len(d['hum']):>7}")

    # 4c. Rainfall Category
    print("\n--- 4c. Rainfall Category vs Other Parameters ---\n")
    rain_cats = defaultdict(lambda: {'temp': [], 'hum': [], 'wind': []})
    for r in data:
        rain = r['Rainfall_mm']
        if rain == 0:
            cat = "No Rain"
        elif rain <= 50:
            cat = "Light (0-50mm)"
        elif rain <= 150:
            cat = "Moderate (50-150)"
        elif rain <= 300:
            cat = "Heavy (150-300)"
        else:
            cat = "Very Heavy (>300)"
        rain_cats[cat]['temp'].append(r['Temperature_C'])
        rain_cats[cat]['hum'].append(r['Humidity_pct'])
        rain_cats[cat]['wind'].append(r['WindSpeed_kmh'])

    print(f"  {'Rain Category':<20} {'Avg Temp':>10} {'Avg Humidity':>13} {'Avg Wind':>10} {'Count':>7}")
    print(f"  {'-'*20} {'-'*10} {'-'*13} {'-'*10} {'-'*7}")
    for cat in ["No Rain", "Light (0-50mm)", "Moderate (50-150)", "Heavy (150-300)", "Very Heavy (>300)"]:
        if cat in rain_cats:
            d = rain_cats[cat]
            print(f"  {cat:<20} {compute_stats(d['temp'])['avg']:>10.2f} "
                  f"{compute_stats(d['hum'])['avg']:>13.2f} "
                  f"{compute_stats(d['wind'])['avg']:>10.2f} "
                  f"{len(d['temp']):>7}")

    # 4d. Per-Region Correlation
    print("\n--- 4d. Temperature-Humidity Correlation by Region ---\n")
    loc_data = defaultdict(lambda: {'temp': [], 'hum': [], 'rain': []})
    for r in data:
        loc_data[r['Location']]['temp'].append(r['Temperature_C'])
        loc_data[r['Location']]['hum'].append(r['Humidity_pct'])
        loc_data[r['Location']]['rain'].append(r['Rainfall_mm'])

    for loc in sorted(loc_data.keys()):
        th = correlation(loc_data[loc]['temp'], loc_data[loc]['hum'])
        tr = correlation(loc_data[loc]['temp'], loc_data[loc]['rain'])
        print(f"  {loc:<12}  Temp<->Humidity: {th:>8.4f}  |  Temp<->Rainfall: {tr:>8.4f}")


# ==========================================================================
# MAIN
# ==========================================================================
if __name__ == "__main__":

    print("=" * 70)
    print("  WEATHER DATA ANALYSIS — BIG DATA PROJECT")
    print("  (Standalone Python — simulates MapReduce/Hive/Spark output)")
    print("=" * 70)

    # Find the CSV file (same directory as this script)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "weather_data.csv")

    if not os.path.exists(csv_path):
        print(f"ERROR: Cannot find {csv_path}")
        exit(1)

    data = load_data(csv_path)
    print(f"\n[INFO] Loaded {len(data)} records from weather_data.csv")
    print(f"[INFO] Locations: {sorted(set(r['Location'] for r in data))}")
    print(f"[INFO] Seasons:   {sorted(set(r['Season'] for r in data))}")

    # Run all 4 analyses
    seasonal_pattern_analysis(data)
    extreme_weather_analysis(data)
    regional_variation_analysis(data)
    correlation_analysis(data)

    print("\n" + "=" * 70)
    print("  ALL ANALYSES COMPLETE")
    print("=" * 70)
