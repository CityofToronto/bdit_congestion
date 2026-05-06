# Congestion Network

## Background

The congestion network is a grid based road network developed to support congestion and reliability analysis. The network is built using HERE street links, which includes streets classified a minor artieral and above. Segments are divided at at centreline intersections, and traffic signals. The network is refreshed annually following the HERE map update schedule, with each release can be identified with `ver_id` used in all partitioned table for this network.



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
