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
    - Minor arterial and above intersections
    - PXs
- Use roads that are Minor Arterial and Above

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
- Segment level TTI and TT
    - Daily TTI and TT, 1 hour (Partitioned Monthly)
    - Weekly TTI and TT
    - Monthly TTI and TT and BI, percentiles, max, mins, 1 hour (Partitioned Yearly)
    - + Peak Period Aggregations (AM Peak, PM Peak, and Weekend Midday)

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