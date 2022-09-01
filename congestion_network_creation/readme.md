# Congestion Network 2.0

# Background

A MVP version of the congestion network was created two years ago, with the goal of creating grid-based segments using HERE links and use that as a base for automating the creation of maps of congestion and reliability metrics for different time periods and aggregations specifically for the congestion monitoring plan. 

The current version is in `congestion.segments_v6` , and its segment definition is approximately 200m long segments, using here links. Related [issue](https://github.com/CityofToronto/bdit_congestion/issues/40)  and some [documentation](https://github.com/CityofToronto/bdit_congestion/tree/grid/congestion_grid) of how that was created. Along with the network, we have several automatically updated intermediate tables:

- segment level weekly TTI and TT
- citywide level daily TTI

Other than using the congestion network for congestion monitoring purposes, we have also used it for other projects such as activeTO and rapidTO, as well as various travel time data requests.

### Area of improvement:

- Not easily updatable for new HERE map version
- Segment does not start and end at intersections
- Creating corridors for projects and analysis is quite manual
- Intermediate table such as the weekly segment level TTI is not particularly useful for most use cases
- versions are not well documented and there are some confusion about which version to use and why some things depends on old versions

## Goals of congestion network 2.0:

- Segments created from intersections to intersections will be clearly defined
- More useful intermediate tables that are widely useable for data requests and projects
- Minimal human intervention in map version update
- Useful functions to create corridors for data requests and projects based on segments
- Static-ish lookup table between centreline and congestion network (since its int to int)

# 1. Creating the network

Network definition: 

- Segments created from intersection to intersection
- Intersections: 
    - Minor arterial, Major Arterials, and Expressway intersections based on the centreline intersection layer
    - Traffic signals 
- Roads: 
    - Minor Arterial, Major Arterials, and Expressway based on the centreline layer

Note: Make sure all tables have descriptive comments including what map version its using and what ta table is updated with this map version

Output Tables:

- Segments table: `congestion.network_segments`
- Segments and here links look up table: `congestion.network_segments_links`
- Routing network: `congestion.routing_network`, and `congestion.routing_network_nodes`

# 2. Creating the tables

List of tables to create:

- Segment level baseline travel times
- Centreline and segment level look up
- Segment level AADT derived from centreline level AADT
- Traffic pattern

Intermediate Table

- Citywide TTI
    - Daily, 1 hour (Partitioned Yearly)
- Segment Level Travel Times
    - Daily Travel Times, 1 hour (Partitioned Monthly)
    - Table structure:

    | column_name | type    | description                                 | example    |
    |-------------|---------|---------------------------------------------|------------|
    | segment_id  | integer | Unique identifier of each segment           | 1029       |
    | dt          | date    | Date in YYYY-MM-DD format                   | 2020-03-07 |
    | hr          | integer | Hour of the day                                        |      8      |
    | tt          | numeric | Average Travel Time on this   segment in seconds   | 20.13      |
    | num_bins    | integer | The total number of 5-min bins   used for aggregating travel times for this segment  | 23         |
    - Monthly Travel times,1 hour (Partitioned Yearly)
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


# 3. Automation

- Create functions to update intermediate tables
- DAG:
    - Update intermediate table - listens to `pull_here`
    - + Map version update
        - how to insert new nodes when we get new traffic signals
        - 1) Find closest new nodes from previous nodes
        - 2) Reroute for each segment and create the network_segment_link lookup table
        - 3) Update functions to use the new network_segment_link lookup table
        - 4) Backfill days that map version changed
        

## 4. How to transition from the current network to the new one

- analysis on how things change
- backfill stuff
- maybe a lookup between old and new network
- audit table for when the base segment network changes (e.g. new roads)

## 5. Documentation

- How to use intermediate tables and agg
- How to route

# Nice to have

- Create routing functions like text_to_centreline and get_link_btwn_nodes
- Create routing functions that is smart enough to find appropriate start and end nodes for streets that has two separate lines for different direction, and smart enough to route correctly (dealing with directional problems on different intersections, e.g. streets with medians)

## Decision points:

1. What to use as nodes: 
    1. traffic signals? 
2. How to handle variability metrics
3. Nice to haves
4. Publishing to OpenData