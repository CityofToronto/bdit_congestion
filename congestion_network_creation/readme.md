# Congestion Network

## What is the Congestion Network?

The congestion network is a grid based road network developed to support congestion and reliability analysis. The network is built using HERE street links, and are segmented at centreline intersections. 

The network is refreshed annually following the HERE map update schedule. Each release can be identified with the `ver_id` column used in all partitioned table for the congestion network.

The current coverage of the congestion network in the City of Toronto contains approximately ~6000 segments.

![image](https://github.com/user-attachments/assets/3d5dce94-ed0f-4afb-9176-d3c4a64a9905)

## How is a congestion segment defined?

### Intersections

Each segment in the congestion network starts and ends at either:
- an intersection, 
- or at a traffic signal location. 

Only minor arterial, Major Arterials, and Expressway intersections based on the centreline intersection layer would be considered as an intersection. 

Midblock traffic signals that do not have an equivalent centreline intersections are currently not included as an intersection. These locations are reviewed annually during the network refresh process to determine whether a new centreline intersection has been introduced to those midblock traffic signals.


### Road

The congestion network includes roadway segments classified as:
- Minor Arterial, 
- Major Arterials, 
- Expressways.

Some collectors that have been previously requested are included as well. Overpasses, or roads that doesn’t actually intersect in real life are not treated as intersections within the congestion network. (e.g. A segment would not start at an intersection of a road and a river.)

### Open Data centreline conflation

For publishing on Open Data, we conflated the HERE-based network segments to the centreline layer. This allows the public to reference congestion network to any other data sources on open data that are based on centreline. Learn more about how we conflate [here](#conflation-to-the-centreline).


> [!IMPORTANT]
> Before you start joining the following tables to other aggregated stats table, it is importatnt to make sure that the travel time data you are using matches the street network. Refer to the data `here.street_valid_range_path_hm` for the corresponding street version. 
> For example, if you are selecting speed data from 2024-01-01 to 2024-04-01, the corresponding street version is `24_4`. Relevant `ver_id` you should filter with should be `24_4`.  

## Table Structure

### Table: `congestion.congestion_segments`

Partition table that stores versions of the congestion network segments. Each segment represents a roadway section between selected intersections and contains attributes such as direction, length, highway classification, and geometry.


| Column Name    | Data Type          | Description                                               |
| -------------- | ------------------ | --------------------------------------------------------- |
| `segment_id`   | `bigint`           | Unique identifier for the roadway segment.                |
| `start_vid`    | `bigint`           | Starting congestion node id for the segment.                 |
| `end_vid`      | `bigint`           | Ending congestion node id for the segment.                   |
| `total_length` | `double precision` | Total segment length in metres.                    |
| `dir`          | `text`             | Directionality of the segment.                            |
| `highway`      | `boolean`          | Indicates whether the segment is classified as a highway based on centreline's feature code. |
| `geom`         | `geometry`         | Geometry representing the directional network segment.                |
| `ver_id`       | `text`             | Dataset version id used for table partitioning. |


### Table: `congestion.congestion_links`

Network links that make up each congestion segments. These links are based on the HERE map.

| Column Name  | Data Type          | Description                                              |
| ------------ | ------------------ | -------------------------------------------------------- |
| `segment_id` | `bigint`           | Identifier of the associated congestion segment.         |
| `start_vid`  | `bigint`           | Starting congestion node id for the segment. ID.                                |
| `end_vid`    | `bigint`           | Ending congestion node id for the segment.                   |ID.                                  |
| `link_dir`   | `text`             | HERE link_dir that make up the segment |
| `length`     | `double precision` | Length of the link geometry in metres.            |
| `geom`       | `geometry`         | Geometry representing the directional link.              |
| `ver_id`     | `text`             | Dataset version id used for table partitioning.   |

### Table: `congestion.congestion_nodes`

Intersection nodes used as the start and end point for each segments within the congestion network. 

| Column Name | Data Type              | Description                                           |
| ----------- | ---------------------- | ----------------------------------------------------- |
| `node_id`   | `bigint`               | Unique identifier for the graph node.                 |
| `geom`      | `geometry(Point,4326)` | Geographic location of the node in WGS84 coordinates. |
| `ver_id`    | `text`                 | Dataset version id used for table partitioning.     |


### Table: `congestion.congestion_nodes_lookup`

Lookup table connecting congestion network nodes to centreline intersections and traffic signals (px). 

| Column Name             | Data Type              | Description                                                                   |
| ----------------------- | ---------------------- | ----------------------------------------------------------------------------- |
| `node_id`               | `bigint`               | Identifier of the graph node.                                                 |
| `intersection_id`       | `integer`              | Identifier of the associated intersection.                                    |
| `px`                    | `text`                 | External or source-system intersection reference code.                        |
| `intersection_desc`     | `text`                 | Human-readable intersection description.                                      |
| `highest_order_feature` | `text`                 | Highest road classification or feature type associated with the intersection. |
| `node_geom`             | `geometry(Point,4326)` | Geometry of the network node in WGS84 coordinates.                            |
| `int_geom`              | `geometry`             | Geometry representing the associated intersection area or point.              |
| `ver_id`                | `text`                 | Dataset version id used for table partitioning.               |

### Table: `congestion.congestion_centreline`

Lookup table connecting centreline to the congestion network. Includes street names, intersection names, highest road class and centreline_ids that make up each segments.

| Column Name       | Data Type   | Description                                                     |
| ----------------- | ----------- | --------------------------------------------------------------- |
| `segment_id`      | `bigint`    | Unique identifier for the congestion segment.                   |
| `streetname`      | `text`      | Street name associated with the segment.                |
| `from_int`        | `integer`   | Centreline intersection_id of the starting intersection.                        |
| `from_int_desc`   | `text`      | Street name of the starting intersection.        |
| `to_int`          | `integer`   | Centreline intersection_id of the ending intersection.                          |
| `to_int_desc`     | `text`      | Street name of the ending intersection.          |
| `centreline_ids`  | `integer[]` | Array of source centreline IDs associated with the segment.     |
| `centreline_uids` | `text[]`    | Array of unique directional centreline identifiers from the source dataset. |
| `geom`            | `geometry`  | Centreline geometry representing the segment.                        |
| `ver_id`          | `text`      | Dataset version id used for table partitioning.         |

### Table: `congestion.congestion_retired_segments`

Store lookup between retired congestion network segments and their replacement segments across network versions. This table is used to track how segments change between the annual refreshes. 

| Column Name       | Data Type   | Description                                                     |
| ----------------- | ----------- | --------------------------------------------------------------- |
| `old_segment_id`      | `bigint`    | Retired congestion network segment id               |
| `new_segment_ids`      | `bigint[]`      | Updated congestion network segment that replaces the old segment                |
| `old_ver`        | `integer`   | Version id of the retired segment                        |
| `new_ver`   | `text`      | Version id of the new segment
    
## Useful functions

`congestion.get_congestion_segments_btwn_nodes`

Returns the congestion network segments between two network nodes using shortest path routing across the congestion network.

### Input:

| Parameter   | Type      | Description                                          |
| ----------- | --------- | ---------------------------------------------------- |
| `start_vid` | `integer` | Starting congestion network node ID.                 |
| `end_vid`   | `integer` | Ending congestion network node ID.                   |
| `ver_id`    | `text`    | Congestion network version identifier (e.g. `25_1`). |

### Output:
| Output Column  | Type        | Description                                                                  |
| -------------- | ----------- | ---------------------------------------------------------------------------- |
| `start_node`   | `integer`   | Input start node ID.                                                         |
| `end_node`     | `integer`   | Input end node ID.                                                           |
| `segment_list` | `integer[]` | Ordered array of congestion segment IDs forming the route between the nodes. |
| `length`       | `numeric`   | Total combined length in metres.                                                |
| `geom`         | `geometry`  | Combined geometry of the route.                                              |

### Example: 

```sql
SELECT * FROM congestion.get_congestion_segments_btwn_nodes(30363068, 30414684, '25_1')
```

## Useful queries

Use the result of the routing function to select the monthly TTI for each routed segments.

```sql
SELECT monthly_data.*
FROM (
    SELECT unnest(segment_list) segment_id FROM congestion.   get_congestion_segments_btwn_nodes(30363068, 30414684, '25_1')) AS routes
INNER JOIN here_agg.segments_bootstrap_monthly monthly_data USING (segment_id)
WHERE dow_group = 'Mon-Fri'  
    AND mnth = '2026-01-01'
    AND ver_id = '25_1'
``` 

## Conflation to the centreline

To support Open Data publication, the congestion network is conflated to the City of Toronto centreline dataset using a combination of graph contraction and shortest-path routing functions available in pg_routing. 

Congestion network nodes are first matched to centreline intersections using nearest-neighbour matching. Because the congestion network and centreline datasets are constructed differently, additional node consolidation is performed in cases where roadway geometry does not align directly between the two networks. For example, left-turn channels that exist as separate roadway features in the congestion network are often consolidated into the main roadway to match the representation used in the centreline dataset. Once intersections and nodes are consolidated, the network is contracted using the matched centreline intersections to generate continuous routing paths between valid intersections. These contracted paths are then used as the base routing network to conflate each congestion segment. Turn restrictions are applied during routing to prevent paths from traversing non-real (e.g. routing into a river) intersections. Where contraction-based routing is not available, additional shortest-path routing with turn restrictions is used to complete the conflation process.

Example of node consolidation

![image](https://github.com/user-attachments/assets/fdd0ec59-254f-48b7-a8c5-865ad5d6e1ec)

Example of centreline conflation result

![image](https://github.com/user-attachments/assets/a432bdc0-1bfb-4659-a573-e5f432c86b62)
