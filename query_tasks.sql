/*1. Query for calculating the average temperature recorded for each device in the dataset.*/

SELECT device_id, avg(temperature) FROM `cleaned_environment` 
GROUP BY device_id

/*2. Query for identifying the devices with the highest average carbon monoxide levels and retrieving the top 5 devices based on this metric.*/

select device_id,avg(carbon_monoxide) as avg_carbon_monoxide from cleaned_environment 
group by device_id
order by carbon_monoxide DESC
limit 5

/*3. Query for determining the average temperature recorded in the cleaned_environment dataset.*/

select avg(temperature) as avg_temp from cleaned_environment 

/*4. Query for identifying the highest recorded temperature for each device and retrieving the corresponding timestamp and temperature values. */

select device_id, timestamp, max(temperature) from cleaned_environment
group by device_id, timestamp

/* 5. The goal is to identify devices where the temperature has increased from the minimum recorded temperature to the maximum recorded temperature */

select DISTINCT device_id from 
(select device_id,
(min(temperature) over(PARTITION BY device_id)) min_temp,
(max(temperature) over (PARTITION BY device_id)) max_temp
from cleaned_environment) as temp_details

/* 6. The aim is to calculate the exponential moving average (EMA) of the temperature for each device. Retrieve the device ID, timestamp, temperature, and the EMA temperature for the first 10 devices from the 'cleaned_environment' table. The EMA temperature is calculated by partitioning the data based on the device ID, ordering it by the timestamp, and considering all preceding rows up to the current row */

select device_id, timestamp, temperature,
avg(temperature) over (partition by device_id order by timestamp ROWS between unbounded preceding and current row) mov_avg
from cleaned_environment
limit 10

/* 7. Here the objective is to identify the timestamps and devices where the carbon monoxide level exceeds the average carbon monoxide level across all devices. */

select timestamp, device_id from cleaned_environment 
where carbon_monoxide > (select avg(carbon_monoxide) from cleaned_environment)

/* 8. Here the objective is to identify the devices that have recorded the highest average temperature among all the devices in the dataset. */

select DISTINCT device_id,
(avg(temperature) over (partition by device_id)) avg_temp
from cleaned_environment

/* 9. The goal is to calculate the average temperature for each hour of the day, considering data from all devices. */

select distinct hour(timestamp) hour_count,
(avg(temperature) over (order by timestamp)) hour_avg
from cleaned_environment

/*10. A query to identify device(s) in the cleaned environment dataset which have recorded only a single distinct temperature value.*/

select device_id, temperature from cleaned_environment
where temperature not in (select distinct temperature from cleaned_environment)

/* 11. The objective is to identify the devices that have recorded the highest humidity levels. */

select distinct device_id,
(max(humidity) over (partition by device_id)) max_humidity
from cleaned_environment

/* 12. This task requires calculating the average temperature for each device while excluding outliers, which are temperatures beyond 3 standard deviations from the mean. */

SELECT distinct device_id,
(avg(temperature) over (PARTITION BY device_id)) no_outliers
from cleaned_environment
where temperature <= (SELECT avg(temperature) + 3*STDDEV(temperature) from cleaned_environment)
and temperature >= (SELECT avg(temperature) - 3*STDDEV(temperature) from cleaned_environment)

/* 13. The goal is to identify devices that have undergone a sudden change in humidity, where the difference is greater than 50%, within a 30-minute time window. */

select device_id, timestamp, humidity
from (
select device_id, timestamp, humidity,
(lag(humidity) over (partition by device_id order by timestamp)) previous_humidity
from cleaned_environment) subquery
where abs(humidity-previous_humidity) > 0.5
and timestamp > timestamp - interval '30' minute

/* 14. This task involves calculating the average temperature for each device separately for weekdays and weekends. */

select distinct device_id,day_of_week,
(avg(temperature) over (PARTITION BY device_id,day_of_week order by timestamp)) avg_temp
from (select device_id, timestamp, temperature,
CASE
	WHEN weekday(timestamp) < 6 then 'Weekday'
    ELSE 'Weekend'
END AS day_of_week
from cleaned_environment) week_details

/* 15. The objective is to calculate the cumulative sum of temperature for each device, considering the records ordered by timestamp limit to 10. */

select device_id, timestamp, temperature,
(sum(temperature) over (partition by device_id order by timestamp)) temp_sum
from cleaned_environment 
limit 10
