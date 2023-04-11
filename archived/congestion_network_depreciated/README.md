# Congestion Network (Depreciated)

The MVP version of the congestion network is now depreciated and replaced by the congestion network v2.

We created this MVP network grid as a base to aggregate and summarize congestion metrics in the city. This grid divides segments from intersection to intersection, futher broken by a maximum length constraint of 200m wihle making sure those partitions have a relaitvely similar length using a set of logics and merging smaller segments using techniques like: greedy partitioning, best group and simulated annealing. 

## Areas of improvement of this MVP version:
- Not very easily updatable when new map versions come
  - Because of the 200m rule, when base link_dir changed across multiple segments, its hard to update this MVP to use the new map version    

- segments does not necessarily starts and ends at intersections
  - hard to create corridors for analysis and data requests 
- segment level intermediate table is weekly and not as useful
  - analysis periods does not usually starts on a weekly level

## Improvements done in Congestion Network V2

- Segments were created from intersections to intersections
- More useful intermediate tables (daily hourly tables were created) that are widely useable for data requests and projects
- Minimal human intervention in map version update, as segments are from intersections to intersections, and intersections don't tend to change much overtime, we can just re-route with the same set of intersections to get the updated segment links
- Useful functions to create corridors for data requests and projects based on segments
- Static-ish lookup table between centreline and congestion network
