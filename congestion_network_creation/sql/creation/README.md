## README
This folder contains files that were used to create the network segments and other related tables, as well as cleaning sql files.

### Creating network:

Preparing nodes:  
- `selected_intersections.sql` - Contains selected centreline intersection , and traffic signal id (px) that were used in creating the network
- `selected_nodes.sql` - Contains selected here nodes that are intersections and will be used for routing

Preparing for routing:
- `network_routing_grid.sql` - HERE network layer for routing the network, created by eliminating unwanted link_dirs from previous cleaned version. This table got further manual cleaning in QGIS. 
- `routing.sql` - Routing function used to route network segments using `congestion.network_nodes` and `congestion.network_routing_grid` with many-to-many.

Creating segments:
- `creating_network_links_21_1.sql` - SQL used to create the first pass of lookup table between link_dir and segment_id from routed results. For cleaning process, see `cleaning_segments.sql`.  
- `creating_network_segment.sql` - SQL used create the first pass of network segments table `congestion.network_segments`. For cleaning process, see `cleaning_segments.sql`. 
- `network_segment_highway.sql` - SQL used to select the first pass of highway segments. This layer then got manually cleaned and inserted into the `highway` column as a boolean in `congestion.network_segments`.

Conflating to centreline:
- `creating_network_int_px.sql` - SQL used to assign centreline intersection id and/or px to each nodes in `congestion.network_nodes`, and creating the initial table of `congestion.network_int_px_21_1`. For cleaning process, see `cleaning_int_node.sql`. 
- `route_node_w_no_int.sql` - Routing functions used conflate segments to centreline, specifically segments with nodes that has no centreline intersection id, see issue 57 for more comments.
- `segment_centreline_routed_21_1.sql` - Routes centreline using int_id <-> node_id look up table `congestion.network_int_px_21_1`, returns in centreline that makes up each segment


### Cleaning:
- `cleaning_int_node.sql` - Contains query used for the creating, manually cleaning, and some validation on the `congestion.network_int_px_21_1` table.
- `cleaning_segments.sql` - Contains query used for the creating, manually cleaning, and some validation on the `congestion.network_segments`