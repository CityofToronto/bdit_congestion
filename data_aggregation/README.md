# README.md

## Data Pipeline

The intermediate tables generated using the congestion network updates with an automated airflow pipeline that uses an external sensor on the `pull_here` DAG (HERE data pipeline). The `congestion_aggregation` DAG runs daily, and starts once the `pull_here` DAG finished running successfully. The `congestion_aggregation` DAG first run the `aggregate_daily` task which uses a PostgresOperator to execute the function `congestion.generate_network_daily(date)` and generates the average daily travel time per hour for each segment. Once `aggregate_daily` finished running and are marked as `Success`, the next ShortCircuitOperator task `check_dom` checks whether the execution date is the first day of the month. If `check_dom` returns `True`, SQLCheckOperator `check_monthly` task checks if all days from the previous month has data. Finally, if the `check_monthly` task returns `True`, the PostgresOperator `aggregate_monthly` task executes the function `congestion.generate_network_monthly(date)` and generates the monthly average daily travel time per hour for each segment.     


## Aggregating Functions

### Daily travel time per hour

The function `congestion.generate_network_daily(date)` takes an input date and aggregates the average daily travel time per hour for each segment and insert the results into an intermediate table `network_segments_daily`. 

Aggregation steps:

1) Produce estimates of the average travel time for each 1 hour bin for each individual link (link_dir) from 5 min bins

2) Produces estimates of the average travel time for each 1 hour bin for each individual segment (segment_id), where at least 80% of the segment (by distance) has observations at the link (link_dir) level. An unadjusted travel time for each segment is also calculated, along with the summed length of link_dir with data and a `is_valid` true and false boolean tag.  

3) Inserts the segment aggregation into congestion.network_segments_daily


### Monthly travel time per hour

The function `congestion.generate_network_monthly(date)` takes an input date, e.g. `2020-01-01` for Jan 2022, and aggregates the average monthly travel time per hour for each segment and insert the results into an intermediate table `network_segments_monthly`.

Aggregation steps:

1) Produce estimates of the average, median, 85th percentile, 95th percentile, minimum and maximum travel time from daily averages for each individual segments

2) Inserts the segment aggregation into congestion.network_segments_monthly

               
## Intermediate Tables

### Daily Travel times 

Daily travel time data is stored under the table `congestion.network_segments_daily`, which is partitioned monthly using range declarative partitioning on the column `dt`. Partitioned table has a suffix of `_yyyymm` indicating the month of data it stores, e.g. `network_segments_daily_202202`.  

- Table structure:
     | column_name   | type    | description                                 | example    |
     |---------------|---------|---------------------------------------------|------------|
     | segment_id    | integer | Unique identifier of each segment           | 1029       |
     | dt            | date    | Date in YYYY-MM-DD format                   | 2020-03-07 |
     | hr            | integer | Hour of the day                             |      8      |
     | tt            | numeric | Average Travel Time on this segment in seconds   | 20.13      |
     | unadjusted_tt | numeric | Average Travel Time on this segment in seconds adjusted to segment's length where at least 80% link_dir has data | 10.25      |
     | length_w_data | numeric | Unadjusted average Travel Time on this segment in seconds (sum of travel times for each corresponding link_dir with data)   | 151.3      |
     | is_valid      | boolean | Whether the adjusted travel time is based on at least 80% of link_dir with data| true      |
     | num_bins      | integer | The total number of 5-min bins used for aggregating travel times for this segment  | 23         |


### Monthly Travel times 

Monthly travel time data is stored under the table `congestion.network_segments_monthly`, which is partitioned yearly using range declarative partitioning on the column `mth`. 
Partitioned table has a suffix of `_yyyy` indicating the year of data it stores, e.g. `network_segments_monthly_2022`.  

- Table structure:

    | column_name | type    | description                                                                                         | example    |
    |-------------|---------|-----------------------------------------------------------------------------------------------------|------------|
    | segment_id  | integer | Unique identifier of each segment                                                                   | 1029       |
    | mth         | date    | Month in YYYY-MM-DD format                                                                          | 03/17/2020 |
    | hr          | integer | Hour of the day                                                                                     | 8          |
    | day_type    | text    | Identifies weekends and weekdays                                                                    | Weekday    |
    | avg_tt      | numeric | Average Travel Time on this   segment in seconds over each month                                    | 13.25      |
    | median_tt   | numeric | Median Travel Time on this   segment in seconds over each month                                     | 14.2       |
    | pct_85_tt   | numeric | The 85th Percentile Travel Time on this segment in seconds over each   month                        | 16.2       |
    | pct_95_tt   | numeric | The 95th Percentile Travel Time on this segment in seconds over each   month                        | 17.22      |
    | min_tt      | numeric | Minimum Travel Time on this   segment in seconds over each month                                    | 8.13       |
    | max_tt      | numeric | Maximum Travel Time on this   segment in seconds over each month                                    | 20.38      |
    | std_dev     | numeric | Standard Deviation of Travel   Times on this segment in seconds over each month                     | 2.34       |
    | num_bins    | integer | The total number of 5-min bins   used for aggregating travel times for this segment over each month | 50         |
