---> PREPARE PHASE <---

-- How does the table looks like? Headers?
SELECT *
FROM rides
LIMIT 50;

--- DATA CREDIBILITY CHECK

	-- Does the duration of the rides make sense? (Did a graph out of it)
SELECT (ended_at - started_at) AS duration, COUNT(1) AS count
FROM rides
GROUP BY duration
ORDER BY duration;

	-- What percentage of all rides represent the rides between 1 and 60 min?
WITH
range_rides AS (SELECT COUNT(1) AS count
                FROM rides
                WHERE (EXTRACT(epoch FROM ended_at - started_at) / 60.0) >= 1.0 AND (EXTRACT(epoch FROM ended_at - started_at) / 60.0) <= (60.0)),
total_rides AS (SELECT COUNT(*) as total
                FROM rides)
SELECT range_rides.count, total_rides.total,
	   ROUND((range_rides.count/total_rides.total::numeric) * 100, 2) AS percentage
FROM range_rides, total_rides;

	-- Wich are the rides that last more than 1 minute and less than 60 minutes? (Did a graph out of it)
SELECT (ended_at - started_at) AS duration, COUNT(1) AS count
FROM rides
WHERE (EXTRACT(epoch FROM ended_at - started_at) / 60.0) >= 1.0 AND (EXTRACT(epoch FROM ended_at - started_at) / 60.0) <= 60.0
GROUP BY duration
ORDER BY duration;

--- DATA INTEGRITY CHECK

	-- ride ID not null check
SELECT 'ride_id' AS variable, COUNT(*) 
FROM rides 
WHERE ride_id IS NULL;

	-- Are ther any duplicate rides?
SELECT ride_id, COUNT(ride_id)
FROM rides
GROUP BY ride_id
HAVING COUNT(ride_id) > 1;

	-- Do all ride IDs have the same lenght?
SELECT LENGTH(ride_id) AS ride_id_length, COUNT(*) AS count_of_length
FROM rides
GROUP BY ride_id_length
ORDER BY ride_id_length;

	-- How many missing cells are there?
SELECT SUM(null_cells.count) AS missing_cells
FROM table_columns_missing AS null_cells;

	-- To be able to run the previous analysis is necesary to create
	-- a VIEW of a table counting the 'NULL' values for each columns.
CREATE OR REPLACE VIEW table_columns_missing AS 
	SELECT 'ride_id' AS variable, COUNT(*)
	FROM rides 
	WHERE ride_id IS NULL
UNION
	SELECT 'rideable_type' AS variable, COUNT(*)
	FROM rides 
	WHERE rideable_type IS NULL
UNION
	SELECT 'started_at' AS variable, COUNT(*)
	FROM rides 
	WHERE started_at IS NULL
UNION
	SELECT 'ended_at' AS variable, COUNT(*)
	FROM rides 
	WHERE ended_at IS NULL
UNION
	SELECT 'start_station_name' AS variable, COUNT(*)
	FROM rides 
	WHERE start_station_name IS NULL
UNION
	SELECT 'start_station_id' AS variable, COUNT(*)
	FROM rides 
	WHERE start_station_id IS NULL
UNION
	SELECT 'end_station_name' AS variable, COUNT(*)
	FROM rides 
	WHERE end_station_name IS NULL
UNION
	SELECT 'end_station_id' AS variable, COUNT(*)
	FROM rides 
	WHERE end_station_id IS NULL
UNION
	SELECT 'start_lat' AS variable, COUNT(*)
	FROM rides 
	WHERE start_lat IS NULL
UNION
	SELECT 'start_lng' AS variable, COUNT(*)
	FROM rides 
	WHERE start_lng IS NULL
UNION
	SELECT 'end_lat' AS variable, COUNT(*)
	FROM rides 
	WHERE end_lat IS NULL
UNION
	SELECT 'end_lng' AS variable, COUNT(*)
	FROM rides 
	WHERE end_lng IS NULL
UNION
	SELECT 'member_casual' AS variable, COUNT(*)
	FROM rides 
	WHERE member_casual IS NULL;
	
	-- Check how does the VIEW Table looks like. 
SELECT *
FROM table_columns_missing;

	-- Use of a WITH statement to add a column with how much % do
	-- the missing cells represent from the total records
WITH
var_w_null AS (SELECT * FROM table_columns_missing),
total_records AS (SELECT COUNT(*) AS total FROM rides)
SELECT var_w_null.variable,
	   var_w_null.count,
	   ROUND((var_w_null.count/total_records.total::numeric * 100), 2) AS percentage
FROM var_w_null, total_records
ORDER BY percentage DESC;

	-- How many records have null values in the start station ot end station?
	-- How much % is it from the total of records?
WITH
table1 AS (SELECT COUNT(*) AS cant_rows
            FROM rides
            WHERE start_station_name IS NULL OR end_station_name IS NULL),
table2 AS (SELECT COUNT(*) AS total
            FROM rides)
SELECT 'No start or end station' AS null_variable,
	   table1.cant_rows,
	   table2.total,
	   ROUND((table1.cant_rows / table2.total::numeric) * 100, 2) AS percentage
FROM table1, table2;

	-- How many rides started OR finished at 00:00:00?
SELECT ride_id, started_at, ended_at
FROM rides
WHERE (EXTRACT(HOUR FROM started_at) = 0
      AND EXTRACT(MINUTE FROM started_at) = 0
      AND EXTRACT(SECOND FROM started_at) = 0)
      OR (EXTRACT(HOUR FROM ended_at) = 0
      AND EXTRACT(MINUTE FROM ended_at) = 0
      AND EXTRACT(SECOND FROM ended_at) = 0);

	-- Check if the starting and ending date are within a valid range.
	-- There were rides that started in 2022 and ended in 2023. So for the end date value I chose 1st/jan 2023.
SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM rides)) AS percentage
FROM rides
WHERE started_at IS NOT NULL AND ended_at IS NOT NULL
  AND DATE(started_at) BETWEEN '2022-01-01'::DATE AND '2022-12-31'::DATE
  AND DATE(ended_at) BETWEEN '2022-01-01'::DATE AND '2023-01-01'::DATE;

	-- Check if and end date are consistent. End date can't be before start date.
SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM rides)) AS percentage
FROM rides
WHERE started_at IS NOT NULL AND ended_at IS NOT NULL
  AND started_at < ended_at;
  
	-- Amount of cases where end date is before start date.
SELECT COUNT(*)
FROM rides
WHERE started_at IS NOT NULL AND ended_at IS NOT NULL
  AND started_at >= ended_at;


---> PROCESS PHASE <---

	-- Delete from the data set the 531 records where the end date is before the start date.
DELETE FROM rides
WHERE started_at IS NOT NULL
  AND ended_at IS NOT NULL
  AND started_at >= ended_at;

	-- Delete the outlier. Rides which duration is less than 1 minute and more than 60 minutes.
DELETE FROM rides
WHERE (EXTRACT(EPOCH FROM ended_at - started_at) / 60.0) < 1.0
   OR (extract(EPOCH FROM ended_at - started_at) / 60.0) > 60.0;

	-- Add a column with the ride duration calculated.
ALTER TABLE rides
ADD COLUMN duration_ride INTEGER GENERATED ALWAYS AS (EXTRACT(EPOCH FROM (ended_at - started_at))/60) STORED;

	-- Add column with the season the ride took place.
ALTER TABLE rides ADD COLUMN season text;

UPDATE rides SET season =
    CASE
        WHEN EXTRACT(MONTH FROM started_at) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM started_at) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM started_at) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM started_at) IN (9, 10, 11) THEN 'Fall'
    END;


---> ANALYSE PHASE <---

	-- 1. How many rides did members and casual riders do? When is it used the most? (by users + total)
        -- Annually
SELECT DATE_PART('year', started_at) AS year,
       COUNT(CASE WHEN member_casual = 'member' THEN 1 END) AS member_count,
       ROUND(100.0 * COUNT(CASE WHEN member_casual = 'member' THEN 1 END) / COUNT(*), 2) AS member_percentage,
       COUNT(CASE WHEN member_casual = 'casual' THEN 1 END) AS casual_count,
       ROUND(100.0 * COUNT(CASE WHEN member_casual = 'casual' THEN 1 END) / COUNT(*), 2) AS casual_percentage,
	   (COUNT(CASE WHEN member_casual = 'member' THEN 1 END) + COUNT(CASE WHEN member_casual = 'casual' THEN 1 END)) AS total
FROM rides
GROUP BY year;
        -- Seasonal
            -- Total rides per season
SELECT season,
       COUNT(*) AS count,
       (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM rides))::numeric(5,2) AS percentage
FROM rides
GROUP BY season
ORDER BY count DESC;
            -- Total rides per season by user type
SELECT season,
       COUNT(*) as total_rides,
       COUNT(CASE WHEN member_casual = 'member' THEN 1 END) AS member_count,
       ROUND(100.0 * COUNT(CASE WHEN member_casual = 'member' THEN 1 END) / COUNT(*), 2) AS member_percentage,
       COUNT(CASE WHEN member_casual = 'casual' THEN 1 END) AS casual_count,       
       ROUND(100.0 * COUNT(CASE WHEN member_casual = 'casual' THEN 1 END) / COUNT(*), 2) AS casual_percentage
FROM rides
GROUP BY season
ORDER BY total_rides DESC;
        -- Monthly
            -- Total rides by month
SELECT to_char(started_at, 'Month') AS month,
       COUNT(*) AS count,
       (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM rides))::numeric(5,2) AS percentage
FROM rides
GROUP BY EXTRACT(MONTH FROM started_at), month
ORDER BY EXTRACT(MONTH FROM started_at);
            -- Totalrides by month by user type
SELECT to_char(started_at, 'Month') AS month,
       COUNT(*) as total_rides,
       COUNT(CASE WHEN member_casual = 'member' THEN 1 END) AS member_count,
       ROUND(100.0 * COUNT(CASE WHEN member_casual = 'member' THEN 1 END) / COUNT(*), 2) AS member_percentage,
       COUNT(CASE WHEN member_casual = 'casual' THEN 1 END) AS casual_count,       
       ROUND(100.0 * COUNT(CASE WHEN member_casual = 'casual' THEN 1 END) / COUNT(*), 2) AS casual_percentage
FROM rides
GROUP BY EXTRACT(MONTH FROM started_at), month
ORDER BY EXTRACT(MONTH FROM started_at);

	-- 2. What day of the week are the bikes used the most (mode)?
        -- General annually
SELECT to_char(started_at, 'Day') AS day_of_the_week,
       COUNT(*)
FROM rides
GROUP BY EXTRACT(DOW FROM started_at), day_of_the_week
ORDER BY EXTRACT(DOW FROM started_at);
        -- General seasonal
SELECT 
    to_char(started_at, 'Day') AS DOW,
    COUNT(CASE WHEN season = 'Winter' THEN 1 END) AS winter_count,
    COUNT(CASE WHEN season = 'Spring' THEN 1 END) AS spring_count,
    COUNT(CASE WHEN season = 'Summer' THEN 1 END) AS summer_count,
	COUNT(CASE WHEN season = 'Fall' THEN 1 END) AS fall_count
FROM rides
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);
        	-- Mode day of the week in Winter
SELECT 
    to_char(started_at, 'Day') AS DOW,
    COUNT(*) AS winter_count
FROM rides
WHERE season = 'Winter'
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY COUNT(*) DESC;
        	-- Mode day of the week in Spring
SELECT 
    to_char(started_at, 'Day') AS DOW,
    COUNT(*) AS spring_count
FROM rides
WHERE season = 'Spring'
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY COUNT(*) DESC;
			-- Mode day of the week in Summer
SELECT 
    to_char(started_at, 'Day') AS DOW,
    COUNT(*) AS summer_count
FROM rides
WHERE season = 'Summer'
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY COUNT(*) DESC;
        	-- Mode day of the week in Fall
SELECT 
    to_char(started_at, 'Day') AS DOW,
    COUNT(*) AS fall_count
FROM rides
WHERE season = 'Fall'
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY COUNT(*) DESC;

	-- 3. How is the distribution by users through the days of the week? 
		-- By users annually
SELECT to_char(started_at, 'Day') AS DOW,
       COUNT(CASE WHEN member_casual = 'member' THEN 1 END) AS member_rides,
       COUNT(CASE WHEN member_casual = 'casual' THEN 1 END) AS casual_rides
FROM rides
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);
		-- Seasonal
			-- By users Winter
SELECT 
    to_char(started_at, 'Day') AS DOW,
    COUNT(CASE WHEN season = 'Winter' AND member_casual = 'member' THEN 1 END) AS member_winter_count,
    COUNT(CASE WHEN season = 'Winter' AND member_casual = 'casual' THEN 1 END) AS casual_winter_count
FROM rides
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);
			-- By users Spring
SELECT 
    to_char(started_at, 'Day') AS DOW,
    COUNT(CASE WHEN season = 'Spring' AND member_casual = 'member' THEN 1 END) AS member_spring_count,
    COUNT(CASE WHEN season = 'Spring' AND member_casual = 'casual' THEN 1 END) AS casual_spring_count
FROM rides
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);
			-- By users Summer
SELECT 
    to_char(started_at, 'Day') AS DOW,
    COUNT(CASE WHEN season = 'Summer' AND member_casual = 'member' THEN 1 END) AS member_summer_count,
    COUNT(CASE WHEN season = 'Summer' AND member_casual = 'casual' THEN 1 END) AS casual_summer_count
FROM rides
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);
			-- By users Fall
SELECT 
    to_char(started_at, 'Day') AS DOW,
    COUNT(CASE WHEN season = 'Fall' AND member_casual = 'member' THEN 1 END) AS member_fall_count,
    COUNT(CASE WHEN season = 'Fall' AND member_casual = 'casual' THEN 1 END) AS casual_fall_count
FROM rides
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);

	-- 4. What is the most common ride length? (Mode)
        -- a. Annually by users
SELECT duration_ride,
       COUNT(CASE WHEN member_casual = 'member' THEN 1 END) AS member_count,
	   COUNT(CASE WHEN member_casual = 'casual' THEN 1 END) AS casual_count
FROM rides
GROUP BY duration_ride
ORDER BY duration_ride;
        -- Seasonal
			-- Winter by users
SELECT duration_ride,
       COUNT(CASE WHEN season = 'Winter' AND member_casual = 'member' THEN 1 END) AS member_winter_count,
	   COUNT(CASE WHEN season = 'Winter' AND member_casual = 'casual' THEN 1 END) AS casual_winter_count
FROM rides
GROUP BY duration_ride
ORDER BY duration_ride;
			-- Spring by users
SELECT duration_ride,
	   COUNT(CASE WHEN season = 'Spring' AND member_casual = 'member' THEN 1 END) AS member_spring_count,
	   COUNT(CASE WHEN season = 'Spring' AND member_casual = 'casual' THEN 1 END) AS casual_spring_count
FROM rides
GROUP BY duration_ride
ORDER BY duration_ride;
			-- Summer by users
SELECT duration_ride,
	   COUNT(CASE WHEN season = 'Summer' AND member_casual = 'member' THEN 1 END) AS member_summer_count,
	   COUNT(CASE WHEN season = 'Summer' AND member_casual = 'casual' THEN 1 END) AS casual_summer_count
FROM rides
GROUP BY duration_ride
ORDER BY duration_ride;
			-- Fall by users
SELECT duration_ride,
	   COUNT(CASE WHEN season = 'Fall' AND member_casual = 'member' THEN 1 END) AS member_fall_count,
	   COUNT(CASE WHEN season = 'Fall' AND member_casual = 'casual' THEN 1 END) AS casual_fall_count
FROM rides
GROUP BY duration_ride
ORDER BY duration_ride;
		-- Monthly
SELECT to_char(started_at, 'Month') as month,
       mode() WITHIN GROUP (ORDER BY CASE WHEN member_casual = 'member' THEN duration_ride END DESC) AS most_common_duration_member,
       mode() WITHIN GROUP (ORDER BY CASE WHEN member_casual = 'casual' THEN duration_ride END DESC) AS most_common_duration_casual
FROM rides
GROUP BY EXTRACT(MONTH FROM started_at), month
ORDER BY EXTRACT(MONTH FROM started_at);
        -- Day of the week
			-- Winter
SELECT to_char(started_at, 'Day') as DOW,
       mode() WITHIN GROUP (ORDER BY CASE WHEN member_casual = 'member' THEN duration_ride END DESC) AS winter_most_common_duration_member,
       mode() WITHIN GROUP (ORDER BY CASE WHEN member_casual = 'casual' THEN duration_ride END DESC) AS winter_most_common_duration_casual
FROM rides
WHERE season = 'Winter'
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);
			-- Spring
SELECT to_char(started_at, 'Day') as DOW,
       mode() WITHIN GROUP (ORDER BY CASE WHEN member_casual = 'member' THEN duration_ride END DESC) AS spring_most_common_duration_member,
       mode() WITHIN GROUP (ORDER BY CASE WHEN member_casual = 'casual' THEN duration_ride END DESC) AS spring_most_common_duration_casual
FROM rides
WHERE season = 'Spring'
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);
			-- Summer
SELECT to_char(started_at, 'Day') as DOW,
       mode() WITHIN GROUP (ORDER BY CASE WHEN member_casual = 'member' THEN duration_ride END DESC) AS summer_most_common_duration_member,
       mode() WITHIN GROUP (ORDER BY CASE WHEN member_casual = 'casual' THEN duration_ride END DESC) AS summer_most_common_duration_casual
FROM rides
WHERE season = 'Summer'
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);
			-- Fall
SELECT to_char(started_at, 'Day') as DOW,
       mode() WITHIN GROUP (ORDER BY CASE WHEN member_casual = 'member' THEN duration_ride END DESC) AS fall_most_common_duration_member,
       mode() WITHIN GROUP (ORDER BY CASE WHEN member_casual = 'casual' THEN duration_ride END DESC) AS fall_most_common_duration_casual
FROM rides
WHERE season = 'Fall'
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);

	-- 5. How long is the average ride duration for annual members and casual riders?
		-- Annually
SELECT member_casual,
       ROUND(AVG(duration_ride), 1) AS average
FROM rides
GROUP BY member_casual
ORDER BY average DESC;

		-- Seasonal
SELECT season,
       ROUND(AVG(CASE WHEN member_casual = 'member' THEN duration_ride ELSE NULL END), 1) AS avg_member_duration,
       ROUND(AVG(CASE WHEN member_casual = 'casual' THEN duration_ride ELSE NULL END), 1) AS avg_casual_duration
FROM rides
GROUP BY season
ORDER BY avg_casual_duration DESC;

		-- Monthly
SELECT to_char(started_at, 'Month') AS month,
       ROUND(AVG(CASE WHEN member_casual = 'member' THEN duration_ride ELSE NULL END), 1) AS avg_member_duration,
       ROUND(AVG(CASE WHEN member_casual = 'casual' THEN duration_ride ELSE NULL END), 1) AS avg_casual_duration
FROM rides
GROUP BY EXTRACT(MONTH FROM started_at), month
ORDER BY EXTRACT(MONTH FROM started_at);

		-- Day of the week
			-- Winter
SELECT to_char(started_at, 'Day') AS DOW,
       ROUND(AVG(CASE WHEN season = 'Winter' AND member_casual = 'member' THEN duration_ride ELSE NULL END), 1) AS member_avg_winter,
       ROUND(AVG(CASE WHEN season = 'Winter' AND member_casual = 'casual' THEN duration_ride ELSE NULL END), 1) AS casual_avg_winter
FROM rides
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);
			-- Spring
SELECT to_char(started_at, 'Day') AS DOW,
       ROUND(AVG(CASE WHEN season = 'Spring' AND member_casual = 'member' THEN duration_ride ELSE NULL END), 1) AS member_avg_spring,
       ROUND(AVG(CASE WHEN season = 'Spring' AND member_casual = 'casual' THEN duration_ride ELSE NULL END), 1) AS casual_avg_spring
FROM rides
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);
			-- Summer
SELECT to_char(started_at, 'Day') AS DOW,
       ROUND(AVG(CASE WHEN season = 'Summer' AND member_casual = 'member' THEN duration_ride ELSE NULL END), 1) AS member_avg_summer,
       ROUND(AVG(CASE WHEN season = 'Summer' AND member_casual = 'casual' THEN duration_ride ELSE NULL END), 1) AS casual_avg_summer
FROM rides
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);
			-- Fall
SELECT to_char(started_at, 'Day') AS DOW,
       ROUND(AVG(CASE WHEN season = 'Fall' AND member_casual = 'member' THEN duration_ride ELSE NULL END), 1) AS member_avg_fall,
       ROUND(AVG(CASE WHEN season = 'Fall' AND member_casual = 'casual' THEN duration_ride ELSE NULL END), 1) AS casual_avg_fall
FROM rides
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);

	-- 6. What type of bicycles do users use? Are there any differences of choice between members and casual riders?
		-- Annually
SELECT EXTRACT(YEAR FROM started_at) AS year,
       COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'classic_bike' THEN 1 END) AS member_classic_rides,
	   COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'classic_bike' THEN 1 END) AS casual_classic_rides,
	   COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'electric_bike' THEN 1 END) AS member_electric_rides,
	   COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'electric_bike' THEN 1 END) AS casual_electric_rides,
       COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'docked_bike' THEN 1 END) AS member_docked_rides,
       COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'docked_bike' THEN 1 END) AS casual_docked_rides
FROM rides
GROUP BY year;
		-- Seasonal
SELECT season,
       COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'classic_bike' THEN 1 END) AS member_classic_rides,
	   COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'classic_bike' THEN 1 END) AS casual_classic_rides,
	   COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'electric_bike' THEN 1 END) AS member_electric_rides,
	   COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'electric_bike' THEN 1 END) AS casual_electric_rides,
       COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'docked_bike' THEN 1 END) AS member_docked_rides,
       COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'docked_bike' THEN 1 END) AS casual_docked_rides
FROM rides
GROUP BY season;
		-- Monthly
SELECT to_char(started_at, 'Month') AS month_name,
       COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'classic_bike' THEN 1 END) AS member_classic_rides,
	   COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'classic_bike' THEN 1 END) AS casual_classic_rides,
	   COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'electric_bike' THEN 1 END) AS member_electric_rides,
	   COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'electric_bike' THEN 1 END) AS casual_electric_rides,
       COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'docked_bike' THEN 1 END) AS member_docked_rides,
       COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'docked_bike' THEN 1 END) AS casual_docked_rides
FROM rides
GROUP BY EXTRACT(MONTH FROM started_at), month_name
ORDER BY EXTRACT(MONTH FROM started_at);

	-- 7. How much time have the bikes been used in total?
		-- Annually
SELECT ROUND(SUM(CASE WHEN member_casual = 'member' THEN duration_ride END)/60::numeric, 1) AS member_total_hours_used,
	   ROUND(SUM(CASE WHEN member_casual = 'casual' THEN duration_ride END)/60::numeric, 1) AS casual_total_hours_used
FROM rides;

		-- Seasonal
SELECT CASE WHEN season = 'Winter' THEN 1
			WHEN season = 'Spring' THEN 2
			WHEN season = 'Summer' THEN 3
			WHEN season = 'Fall' THEN 4
			END AS season_num,
	   season,
	   ROUND(SUM(CASE WHEN member_casual = 'member' THEN duration_ride END)/60::numeric, 1) AS member_total_hours_used,
	   ROUND(SUM(CASE WHEN member_casual = 'casual' THEN duration_ride END)/60::numeric, 1) AS casual_total_hours_used
FROM rides
GROUP BY season_num, season;

		-- Monthly
SELECT to_char(started_at, 'Month') AS month_name,
	   ROUND(SUM(CASE WHEN member_casual = 'member' THEN duration_ride END)/60::numeric, 1) AS member_total_hours_used,
	   ROUND(SUM(CASE WHEN member_casual = 'casual' THEN duration_ride END)/60::numeric, 1) AS casual_total_hours_used
FROM rides
GROUP BY EXTRACT(MONTH FROM started_at), month_name
ORDER BY EXTRACT(MONTH FROM started_at);

		-- Day of the week
			-- Winter
SELECT to_char(started_at, 'Day') AS DOW,
       ROUND(SUM(CASE WHEN season = 'Winter' AND member_casual = 'member' THEN duration_ride END)/60::numeric, 1) AS member_winter_total_hours_used,
	   ROUND(SUM(CASE WHEN season = 'Winter' AND member_casual = 'casual' THEN duration_ride END)/60::numeric, 1) AS casual_winter_total_hours_used
FROM rides
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);
			-- Spring
SELECT to_char(started_at, 'Day') AS DOW,
       ROUND(SUM(CASE WHEN season = 'Spring' AND member_casual = 'member' THEN duration_ride END)/60::numeric, 1) AS member_spring_total_hours_used,
	   ROUND(SUM(CASE WHEN season = 'Spring' AND member_casual = 'casual' THEN duration_ride END)/60::numeric, 1) AS casual_spring_total_hours_used
FROM rides
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);
			-- Summer
SELECT to_char(started_at, 'Day') AS DOW,
       ROUND(SUM(CASE WHEN season = 'Summer' AND member_casual = 'member' THEN duration_ride END)/60::numeric, 1) AS member_summer_total_hours_used,
	   ROUND(SUM(CASE WHEN season = 'Summer' AND member_casual = 'casual' THEN duration_ride END)/60::numeric, 1) AS casual_summer_total_hours_used
FROM rides
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);
			-- Fall
SELECT to_char(started_at, 'Day') AS DOW,
       ROUND(SUM(CASE WHEN season = 'Fall' AND member_casual = 'member' THEN duration_ride END)/60::numeric, 1) AS member_fall_total_hours_used,
	   ROUND(SUM(CASE WHEN season = 'Fall' AND member_casual = 'casual' THEN duration_ride END)/60::numeric, 1) AS casual_fall_total_hours_used
FROM rides
GROUP BY EXTRACT(DOW FROM started_at), DOW
ORDER BY EXTRACT(DOW FROM started_at);