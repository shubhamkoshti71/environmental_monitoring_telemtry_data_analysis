# Environmental Monitoring Project: Telemetry Data Analysis

## Project Overview
This project is focused on analyzing environmental telemetry data collected from various devices. Using Python for data processing and SQL for querying, the aim is to gain insights into temperature, carbon monoxide levels, and humidity to ensure proper monitoring of environmental conditions across devices.

## Key Components
- **Module.py**: Python script responsible for data cleaning and preprocessing of telemetry data.
- **query_tasks.sql**: SQL script containing multiple queries to analyze the cleaned data, revealing trends, patterns, and key insights.
- **Tasks description_module_code.txt**: Document describing the purpose of each component and detailing the functionalities of the Python module and SQL queries.

## Insights:
1. **Temperature Analysis**:
   - Calculated the average temperature recorded by each device.
   - Identified the highest temperature recorded for each device, along with the corresponding timestamps.
   - Analyzed the average temperature over different times of the day (e.g., hourly averages), and separated data for weekdays and weekends to observe trends.

2. **Carbon Monoxide Levels**:
   - Identified devices with the highest average carbon monoxide levels and retrieved the top 5 devices based on this metric.
   - Queried devices where the carbon monoxide levels exceeded the average levels recorded across all devices.

3. **Humidity Analysis**:
   - Calculated the maximum humidity levels recorded for each device.
   - Identified devices with sudden changes in humidity levels (greater than 50%) within a 30-minute time window, highlighting potential environmental anomalies.

4. **Data Filtering for Anomalies**:
   - Excluded outliers from temperature analysis by filtering values beyond three standard deviations from the mean, ensuring more accurate environmental readings.
   - Identified devices recording only a single distinct temperature value, possibly indicating sensor malfunctions.

## SQL Query Highlights
- **Exponential Moving Average (EMA)**: Calculated the EMA of temperature readings over time for each device to smooth the time-series data and understand trends.
- **Cumulative Sum of Temperature**: Computed the cumulative sum of temperature values for each device, allowing for a longitudinal view of temperature trends.
- **Device Performance**: Queried the devices that have recorded significant temperature increases from the minimum to maximum values, providing insights into device variability and performance.

## Usage
- Run `Module.py` to clean and preprocess the telemetry data.
- Use the queries in `query_tasks.sql` to analyze the cleaned data and extract environmental insights.
- Refer to `Tasks description_module_code.txt` for a detailed breakdown of the code functionality.

