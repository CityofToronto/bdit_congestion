# README.md

## Data Pipeline

The congestion data pipeline consists of two DAGs in Airflow:

### pull_here DAG

This DAG runs daily and automates the ingestion of here data for the entry city.

### congestion_aggregation DAG

This DAG runs daily and is triggered after the successful execution of the pull_here DAG. The task `aggregate_daily`, runs the function `congestion.generate_network_daily_spd(date)` which aggregated segment level daily hour speed.


## Aggregating Functions

### Daily speed per hour

The function `congestion.generate_network_daily_spd(date)` takes an input date and aggregates the average daily speed per hour for each segment and insert the results into an intermediate table `network_segments_daily_spd`. 

Aggregation steps:

1) Link-Level Aggregation

- From HERE 5-minute bins, average speed is calculated for each 1-hour bin per link_dir

2) Segment-Level Aggregation

- Merge link-level speeds for all link_dirs that make up each segment_id.

- Ensure at least 80% of the segment length has valid speed observations (`is_valid` boolean flag).

- Calculates the average speed as a length-weighted harmonic mean. 

3) Inserts data with retired segment logic

- The results are inserted into `congestion.network_segments_daily_spd` with logic to include retired segments, as long as the input date falls within their valid range. This allows historical data to be included even for segments that are no longer active. 

- For example, segment A was active in the map version 22_2, but after map version 23_4, there is a new traffic signal that would split segment A into two segments (segment B and C). Segment A would then be retired, and will only have data till the day it was retired. Starting from map version 23_4, segment A will no long have data in `congestion.network_segments_daily_spd`, and will be replaced with data for segment B and C. However, for dates prior to the map changing to 23_4, we still need data for Segment A. See more about retiring segments [here](../congestion_network_creation/sql/update/README.md).

Pre-aggregation checks:

1) HERE data availability: Checks that the input date has data in the `here.ta_path` table

2) Date bounds: Ensures the input date is with the expected date ranges.

3) Street version existence: Determines the correct street version to use based on the input date and confirms its the street version table exists in the schema.


## Intermediate Tables

### Daily Hourly Speed

Daily hourly speed data is stored under the table `congestion.network_segments_daily_spd`, which is partitioned yearly, and then monthly using range declarative partitioning on the column `dt`. Partitioned table has a suffix of `_yyyymm` indicating the month of data it stores, e.g. `network_segments_daily_spd_202202`. 

This table includes speed information for both active and retired segments, as long as the input date falls within the valid date range of the retired segment.

Table structure:

| column_name    | type    | description                                                                                 | example    |
| --------------- | ------- | ------------------------------------------------------------------------------------------- | ---------- |
| segment_id     | integer | Unique identifier of each segment                                                           | 1029       |
| dt              | date    | Date in `YYYY-MM-DD` format                                                                 | 2020-03-07 |
| hr              | integer | Hour of the day (0–23)                                                                      | 8          |
| spd             | numeric | Average speed (in km/h) calculated as a length-weighted harmonic mean over valid link_dirs | 36.57      |
| length_w_data | numeric | Sum of the lengths (in meters) of link_dirs with valid speed data                          | 151.3      |
| total_length   | numeric | Total length (in meters) of the segment (based on current or retired geometry)              | 180.0      |
| is_valid       | boolean | Whether at least 80% of the segment (by length) had valid link_dir data                    | true       |
| num_bin        | integer | Total number of 5-minute bins used across all link_dirs for this segment/hour              | 23         |


### Derived Travel Times

Travel time estimates are provided with the view `congestion.travel_time_daily`, which calculates hourly travel times by using daily hourly speed from  `network_segments_daily_spd` with the appropriate segment length (from current or retired tables).

View structure:
| column_name | type    | description                                                                 | example    |
| ------------ | ------- | --------------------------------------------------------------------------- | ---------- |
| segment_id  | integer | Unique identifier of the segment                                            | 1029       |
| dt           | date    | Date in `YYYY-MM-DD` format                                                 | 2020-03-07 |
| hr           | integer | Hour of the day (0–23)                                                      | 8          |
| tt           | numeric | Travel time (in seconds)          | 17.7       |
| is_valid    | boolean | Whether the travel time is valid (i.e., based on at least 80% segment data) | true       |
| num_bin     | integer | Total number of 5-minute bins used for this aggregation                     | 23         |
