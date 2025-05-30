# Methodology of creating the congestion network 2.0

## Select intersections for routing
First select intersections that are [minor arterial and above](selected_intersections.sql), as well as all [traffic signals](selected_nodes.sql) as nodes for routing. Then use a buffer to filter out intersections and traffic signals that are not on the selected road classes. Using [nearest neighbour](creating_network_nodes.sql), find the closest here nodes for each selected intersection/traffic signal. Then manually validate the matches by comparing the distance of matched node_ids and int_id/px and visualizing on QGIS.  

## Routing and creating the network
First [create a routing network](network_routing_grid.sql) using the last cleaned version to elimiate most unwanted links in the current network. Then use the [network nodes](creating_network_nodes.sql) created in the previous step, with many-to-many [routing function](routing.sql) to route the shortest path for all nodes pair. Routing results are then [cleaned](cleaning_segments.sql), e.g. [merging smaller segments](https://github.com/CityofToronto/bdit_congestion/issues/56#issuecomment-1155398433) and correctly start and end nodes for streets with medians. Once the network is cleaned, it is then validated visually on QGIS, and ran through checks like 1) making sure there are no duplicated link_dirs, 2) making sure all nodes were used in routing, 3) checking the length of the segments etc. Lastly, assign a segment_id as the uniqiue identifier for each routed sets, and create [lookup table](creating_network_links_21_1.sql) and [segment network table](creating_network_segments.sql). 

Final network: 
![image](https://user-images.githubusercontent.com/46324452/175983371-52cd2a1e-9bf7-4246-ae28-12a3e207b7cd.png)


## Conflating to the centreline

First assign network nodes to either centreline intersection or (midblocks) traffic signals. Create a list of centreline intersection and traffic signals we want to match by (1) filtering for feature code `fcode` in centreline, (2) combining traffic signals with intersections, and (3)  finding traffic signals that are not on a centreline intersection (such as midblocks). Then, using nearest neighbour, match each node_id to its closest centreline int_id/traffic signal and filter matches that have a distance of more than 25m. Finally comes the manual part of QCing the matches. Load the matched layer and the node_id layer and manually match the ones that did not get matched. Most matches are correct, other than expressways and streets with median. Compare the node_id's geometry and their assigned int_id's geom and check if they are correct.

After getting the sets of int_id we need for routing centreline, we used one-to-one [routing function](sql/creation/segments_centreline_routed_21_1.sql) to route the shortest path from the start to end int, resulting in sets of centreline (geo_id) that make up each segment. There were cases where some node_id does not have an equivalent int_id due to being a mid-block traffic signal. In those cases, we route centreline [through those nodes](sql/creation/route_node_w_no_int.sql) without int and assign the result centreline set to the equivalent segment_id(s), see more in Issue 57. Both routed results then got validated and clean, and combined into one table. 

### Special Cases
There are many cases where assigning intersection id to nodes are not as straight-forward as we would think. Some examples are listed below, along with the rationale of how we decided to assign intersection id for each example. 

#### Case 1: The congestion network and centreline draw roads differently

In this example below, HERE draws the Scarlett Rd and Dundas Road intersection different than centreline. Centreline represented the intersection with 2 lines, while HERE used Link A, Link B and Link C to represent the intersection (because there is a traffic island). This makes matching a little more complicated as centerline doesn't really have Link A and Link B. After checking the satelitte imagary we can see that node C to node A is the only link representing SB traffic. To ensure the that we capture both bounds' traffic and simplify the network. We assign node A to node E's intersection id, retaining the Node C to Node A link_dir as well as the SB bound traffic. Node B to Node C link_dir was removed as Node E to Node C already represents NB traffic.  


![image](https://user-images.githubusercontent.com/46324452/179607282-b4ee7fb7-3c0f-45c1-b81d-4cfccc57ce4d.png)


#### Case 2: Congestion network is out of date

There are cases where the congestion network (based on HERE) are outdated. 
In the example below, this intersection at Islington Avenue and Rexdale Blvd had a road reconfiguration, with changes including removing right turn channel, realigning and simplifing the intersection. As you can see in the image below, the congestion network still shows the retired right turn channel and other roads, where the centreline is updated to show the most recent changes. In this case, as the network segments no longer represent current traffic network we moved the outdated segments from the network segments table `congestion.network_segments` to the retired network segments table `congestion.network_segments_retired`. The network segments table will not have any segments representing this intersection until the HERE map version refreshes and we update the segments with updated links. See more about the steps to retire outdated segments [here](/congestion_network_creation/sql/update/README.md).


![image](https://user-images.githubusercontent.com/46324452/179259200-d6ab4f9b-d4d3-4fde-9cb0-d889753b79fd.png)

