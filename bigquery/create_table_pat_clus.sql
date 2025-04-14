CREATE OR REPLACE EXTERNAL TABLE `global-rookery-448215-m8.bikesdata_share.bikefulldataset_parquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://global-rookery-448215-m8_bike_data_raw/full_raw_paq/*.parquet']
);

CREATE OR REPLACE TABLE global-rookery-448215-m8.bikesdata_share.bikefulldataset_parquet_non_partitioned AS
SELECT * FROM global-rookery-448215-m8.bikesdata_share.bikefulldataset_parquet;

CREATE OR REPLACE TABLE global-rookery-448215-m8.bikesdata_share.bikefulldataset_partitioned_clustered
PARTITION BY DATE_TRUNC(started_at, MONTH)
CLUSTER BY member_casual, start_station_id AS
SELECT * FROM global-rookery-448215-m8.bikesdata_share.bikefulldataset_parquet;

SELECT COUNT(*) FROM global-rookery-448215-m8.bikesdata_share.bikefulldataset_partitioned_clustered