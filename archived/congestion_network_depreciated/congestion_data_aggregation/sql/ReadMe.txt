# Data source and data flow

## Automatic Pipeline

1) Citywide Travel Time Index
    - Function `congestion.generate_citywide_tti_monthly` insert an averaged monthly hourly citywide travel time index from 7am to 11pm every month. It aggregates travel time on a link level up to segments, filtering the segments that does not have at least 80% of its lengeth worth of links. Highway uses a baseline of 25th percetile of travel time while the rest of the streets use a baseline of 10th percentile of travel time. Highway is defined in `congestion.highway_segments_v5`. This function runs monthly with `congestion_refresh` DAG.
    
2) Citywide Buffer Index
    - Function `congestion.generate_citywide_bi_monthly` insert an averaged monthly hourly citywide buffer index from 7am to 11pm every month. It aggregates travel time on a link level up to segments, filtering the segments that does not have at least 80% of its length worth of links. This function runs monthly with `congestion_refresh` DAG.
    
3) Corridor Travel Time Index
     - Function `congestion.generate_corridor_tti_monthly` insert an averaged monthly hourly corridor travel time index from 7am to 11pm every month for each corridor. It first aggregates travel time on a link level up to segments, filtering the segments that does not have at least 80% of its length worth of links. It then aggregates segments up to corridor, also filtering corridors that does not have at least 80% of its length worth of segments. Highway uses a baseline of 25th percetile of travel time while the rest of the streets use a baseline of 10th percentile of travel time. Highway is defined in `congestion.highway_segments_v5`. This function runs monthly with `congestion_refresh` DAG.
     
4) Corridor Buffer Index
     - Function `congestion.generate_corridor_bi_monthly` insert an averaged monthly hourly citywide buffer index from 7am to 11pm every month. It aggregates travel time on a link level up to segments, filtering the segments that does not have at least 80% of its length worth of links. It then aggregates segments up to corridor, also filtering corridors that does not have at least 80% of its length worth of segments. This function runs monthly with `congestion_refresh` DAG.

