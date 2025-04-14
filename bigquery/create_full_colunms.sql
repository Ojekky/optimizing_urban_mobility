CREATE OR REPLACE TABLE `global-rookery-448215-m8.bikesdata_share.bike_trip_full_analysis` AS
SELECT
  ride_id,
  rideable_type,
    -- New columns start here
  started_at,
  ended_at,
  DATE(started_at) AS trip_date,
  EXTRACT(YEAR FROM started_at) AS trip_year,
  FORMAT_DATE('%Y-%m', DATE(started_at)) AS trip_month,
  EXTRACT(HOUR FROM started_at) AS trip_hour,
  CASE 
    WHEN EXTRACT(DAYOFWEEK FROM started_at) IN (1,7) THEN 'Weekend'
    ELSE 'Weekday'
  END AS trip_weekday_weekend,
  -- Trip characteristics
  start_station_id,
  start_station_name,
  end_station_id,
  end_station_name,
  CASE 
    WHEN start_station_id = end_station_id THEN 'Yes'
    ELSE 'No'
  END AS is_round_trip,
  CONCAT(start_station_name, ' -> ', end_station_name) AS trip_route,
  member_casual,
  -- Approximate trip distance in km
  start_lat,
  start_lng,
  end_lat,
  end_lng,
  ROUND(
    ST_DISTANCE(
      ST_GEOGPOINT(start_lng, start_lat),
      ST_GEOGPOINT(end_lng, end_lat)
    ) / 1000, 2
  ) AS trip_distance_km,
  -- Geodistance category
  CASE
    WHEN ST_DISTANCE(ST_GEOGPOINT(start_lng, start_lat), ST_GEOGPOINT(end_lng, end_lat)) / 1000 < 1 THEN 'Under 1km'
    WHEN ST_DISTANCE(ST_GEOGPOINT(start_lng, start_lat), ST_GEOGPOINT(end_lng, end_lat)) / 1000 BETWEEN 1 AND 3 THEN '1-3km'
    WHEN ST_DISTANCE(ST_GEOGPOINT(start_lng, start_lat), ST_GEOGPOINT(end_lng, end_lat)) / 1000 BETWEEN 3 AND 5 THEN '3-5km'
    ELSE 'Over 5km'
  END AS geodistance_category,
   -- Usage categories by duration
  trip_duration,
  CASE
    WHEN trip_duration IS NULL THEN NULL
    WHEN trip_duration < 300 THEN 'Very Short'
    WHEN trip_duration BETWEEN 300 AND 600 THEN 'Short'
    WHEN trip_duration BETWEEN 600 AND 900 THEN 'Medium'
    ELSE 'Long'
  END AS usage_category 
FROM `global-rookery-448215-m8.bikesdata_share.bikefulldataset_partitioned_clustered`;