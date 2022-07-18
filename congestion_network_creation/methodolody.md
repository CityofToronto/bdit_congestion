# Methodology of assigning nodes to either centreline intersection or (midblocks) traffic signals 
First create a list of centreline intersection and traffic signals we want to match by (1) filtering for feature code `fcode` in centreline, (2) combining traffic signals with intersections, and (3)  finding traffic signals that are not on a centreline intersection (such as midblocks). Then, using nearest neighbour, match each node_id to its closest centreline int_id/traffic signal and filter matches that have a distance of more than 25m. Finally comes the manual part of QCing the matches. Load the matched layer and the node_id layer and manually match the ones that did not get matched. Most matches are correct, other than expressways and streets with median. Compare the node_id's geometry and their assigned int_id's geom and check if they are correct.

## Special Cases
There are many cases where assigning intersection id to nodes are not as straight-forward as we would think. Some examples are listed below, along with the rationale of how we decided to assign intersection id for each example. 

### Case 1: The congestion network and centreline draw roads differently

In this example below, HERE draws the Scarlett Rd and Dundas Road intersection different than centreline. Centreline represented the intersection with 2 lines, while HERE used Link A, Link B and Link C to represent the intersection (because there is a traffic island). This makes matching a little more complicated as centerline doesn't really have Link A and Link B. After checking the satelitte imagary we can see that node C to node A is the only link representing SB traffic. To ensure the that we capture both bounds' traffic and simplify the network. We assign node A to node E's intersection id, retaining the Node C to Node A link_dir as well as the SB bound traffic. Node B to Node C link_dir was removed as Node E to Node C already represents NB traffic.  


![image](https://user-images.githubusercontent.com/46324452/179607282-b4ee7fb7-3c0f-45c1-b81d-4cfccc57ce4d.png)


### Case 2: Congestion network is out of date

There are cases where the congestion network (based on HERE) are outdated. 
In the example below, this intersection at Islington Avenue and Rexdale Blvd had a road reconfiguration, with changes including removing right turn channel, realigning and simplifing the intersection. As you can see in the image below, the congestion network still shows the retired right turn channel and other roads, where the centreline is updated to show the most recent changes. In this case, as the network segments no longer represent current traffic network we moved the outdated segments from the network segments table `congestion.network_segments` to the retired network segments table `congestion.network_segments_retired`. The network segments table will not have any segments representing this intersection until the HERE map version refreshes and we update the segments with updated links. See more about the steps to retire outdated segments here.


![image](https://user-images.githubusercontent.com/46324452/179259200-d6ab4f9b-d4d3-4fde-9cb0-d889753b79fd.png)
