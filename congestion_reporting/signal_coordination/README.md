# Signal Coordination

*Note: This page should be updated for a more comprehensive overview of the process.

### Definitions

**Corridors** - signal-to-signal lengths of road

**Groups** - groupings of corridors that typically make up the entire length of road being re-timed

### Steps

**1. Populate here_analysis.corridors**

Represents lookup table for all corridors (i.e. signal-to-signal lengths of road)

`corridor_id`: unique identifier

`corridor_name`: `street` || ' ' || `direction` || ' - ' || `intersection_start` || ' to ' || `intersection_end`

`length_km`: total length of corridor, in kilometers

`street`: Street

`direction`: 2-letter direction

`intersection_start`: Starting intersection

`intersection_end`: End intersection

`group_id`: identifier for group of corridors, manually updated at this point

`group_order`: sequence of corridor within group, manually updated at this point

`corridor_load_id`: links back to geometries as part of a corridor creation tool, can leave NULL

`num_links`: Number of HERE links that comprise the corridor
