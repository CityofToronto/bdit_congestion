# Signal Coordination

**Note: This page should be updated for a more comprehensive overview of the process.**

## Definitions

**Corridors** - signal-to-signal lengths of road

**Groups** - groupings of corridors that typically make up the entire length of road being re-timed

## Steps

### **1. Populate here_analysis.corridors**

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


### **2. Populate here_analysis.corridor_links**

Intermediate table that links `here.ta.link_dir` to `here_analysis.corridors`

`corridor_link_id`: unique identifier

`corridor_id`: link back to `here.corridors`

`link_dir`: link to `here.ta`

`seq`: order on which specific link is located in corridor (e.g. 1, 2, 3, ... up to `here_analysis.corridors.num_links`

`distance_km`: total length of corresponding `here_gis.streets_yy_m.link_id`

`tot_distance_km`: total cumulative length of corridor, up to and inclusive of current link

### **3. Update links and lengths of `corridors` and `corridor_links` (if manual changes are made)**

Located [here](_sql/update_table/update-table-corridor_links-links_and_lengths.sql)

### **4. Populate `corridor_link_agg`**

Weekday query located [here](_sql/load_table/load-table-corridor_link_agg.sql)

Weekend query located [here](_sql/load_table/load-table-corridor_link_agg_wkend.sql)

### **5. Run output queries**

Weekday query located [here](_sql/report_query/qry-signal_coord_ext.sql)

Weekend query located [here](_sql/report_query/qry-signal_coord_ext_wkend.sql)

### **6. Paste into template**

Example located here: K:\tra\GM Office\Big Data Group\Work\Signal Coordination\2018\GROUP A\Before
