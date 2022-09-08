# Congestion network versioning

Things change, people change, and most importantly roads change. When roads get reconfigured, or when we get new versions of HERE map, we will need to apply changes to various tables to keep our congestion network segments up-to-date. 

## When do we make changes

Changes to the network segments table will be made once a year, when we receive yearly updated HERE map, which is the base network used to create the network segments table. When there are major road reconfiguration that affects certain segments data avaliability, those will be moved to the retired table when necessary. 

## What changes

The yearly update will include creating a new segment_links table for the current HERE map version, a lookup table that links network segments to the latest HERE links. The centreline used for referencing intersection id will also be updated, as well as the traffic signal layer. The segments information in network segments table such as from_node, to_node, from_int, to_int, from_px and to_px will be updated to comply with the latest centreline, traffic signal, and HERE map version. The old segments information will be stored in the retired table, with an `updated` reason, detailing the relationship between the retired segments to other data sources.  

## Steps to update the network:

1) Check if there is new traffic signal installed by looking at the activation date

2) Check what has changed in the new map version using [this SQL](find_changes.sql)
    - `congestion.network_nodes`
    - `congestion.network_links_xx_x`
    - `congestion.network_segments`

3) Update network nodes table using  [this SQL](update_nodes.sql)

4) Create new network_link table by routing the start and end vid of changed segment_ids using [this SQL](update_links.sql)

5) Retire outdated segments to table `congestion.network_segments_retired` using [this]

6) Add new segments if needed using [this SQL](update_segments.sql)

7) Update baseline travel times for new segments using [this SQL](update_segments.sql)

6) Update centreline conflations for nodes and segment lookup table using [this SQL](update_segments.sql)

7) Retire outdated segments to table `congestion.network_segments_retired` using [this SQL](update_retired_segments.sql)

## How to check changes

### Audit Tables
There is an audit table `congestion.logged_actions` that monitors changes in the following tables:
- `congestion.network_nodes`
- `congestion.network_segments`
- `congestion.network_baseline`

Changes such as `INSERT`, `UPDATE`, `DELETE` on those tables will be logged as an action under `congestion.logged_actions`.

### Retired segments
Information on retired segments will be logged in `congestion.network_segments_retired`, which gets updated when a segment is no longer in use.

Table Structure:
| column_name        | type       | description                                                   | example                  |
|--------------------|------------|---------------------------------------------------------------|--------------------------|
| segment_id         | integer    | unique identifier of each segment                             | 3216                     |
| start_vid          | integer    | here node_id that is the source of the segment                | 2516521                  |
| end_vid            | integer    | here node_id that is the target of the segment                | 2516523                  |
| geom               | geometry   | geometry of the retired segment in epsg: 4326                 |                          |
| total_length       | numeric    | length of the retired segment in metres                       | 260.23                   |
| highway            | boolean    | Identifies highway                                            | FALSE                    |
| start_int          | integer    | centreline int_id that is the source of the segment           | 103265413                |
| end_int            | integer    | centreline int_id that is the target of the segment           | 103216546                |
| start_px           | integer    | traffic signal px that is the source of the segment           | 203                      |
| end_px             | integer    | traffic signal px that is the target of the segment           | 204                      |
| here_version       | text       | here versions that have link_dirs for this segment            | `21_1`                   |
| centreline_version | text       | centreline versions that have link_dirs for this segment      | ['20220705`, '20230807`] |
| retired_date       | date       | date this segment was removed from the network_segments table | 08/06/2022               |
| retired_reason     | text       | reasons for retiring this segment                             | outdated                 |
| replaced_id        | int array  | new segment ids replacing this retired segment                | [6500,6501]              |
| valid_from_date    | date       | the ending date that this segment have data                   | 09/01/2017               |
| valid_to_date      | date       | the ending date that this segment have data                   | 08/05/2022               |
