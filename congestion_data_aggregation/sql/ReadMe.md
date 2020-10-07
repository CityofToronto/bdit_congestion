# Data source and data flow

## Airflow DAG

The `congestion_refresh` DAG runs everyday as a child to `pull_here` DAG and has three tasks, 
1) aggregate_citywide_tti,
2) aggregate_segments_tti_weekly, and
3) aggregate_segments_bi_monthly. 

`aggregate_citywide_tti` runs daily as soon as `pull_here` DAG finished pulling daily here data. `aggregate_segmnets_tti_weekly` and `aggregate_segments_bi_monthly` are then run weekly and monthly respectively with the use of `ShortCircuitOperator`. 


1) Citywide Travel Time Index
    - Function `congestion.generate_citywide_tti_daily` insert an averaged daily hourly citywide travel time index from 6am to 11pm every month. It aggregates travel time on a link level up to segments, filtering the segments that does not have at least 80% of its lengeth worth of links. Highway uses a baseline of 25th percetile of travel time while the rest of the streets use a baseline of 10th percentile of travel time. Highway is defined in `congestion.highway_segments_v5`. This function runs daily with `congestion_refresh` DAG.
    
2) Citywide Buffer Index
    - Materialized view `congestion.citywide_bi_monthly` produces an averaged monthly hourly citywide buffer index from 6am to 11pm for each month. It uses `congestion.segment_bi_monthly` updated by function `congestion.generate_segments_bi_monthly` and aggregates segment level buffer index to citywide level. This materialized view refresh monthly with `congestion_refresh` DAG.
    
3) Corridor Travel Time Index
     - View `congestion.corridor_tti_weekly` produces an averaged weekly hourly corridor travel time index from 6am to 11pm for each corridor. It uses `congestion.segment_tti_weekly` updated by function `congestion.generate_segment_tti_weekly` and aggregates segments up to corridor, also filtering corridors that does not have at least 80% of its length worth of segments.

     
4) Corridor Buffer Index
     - View `congestion.corridor_bi_monthly` produces an averaged monthly hourly corridor buffer index from 6am to 11pm each corridor. It uses `congestion.segment_bi_monthly` updated by function `congestion.generate_segment_bi_monthly` and aggregates segments up to corridor, also filtering corridors that does not have at least 80% of its length worth of segments. 
