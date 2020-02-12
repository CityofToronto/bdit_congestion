# Creating a grid
In order to aggregate and summarize congestion metrics, we created a grid that divides segments from intersection to intersection, futher broken by a maximum length constraint of 200m while making sure those partitions have a relatively simliar length.

## Merge small segments
There are a lot of ramps and small street segments that are shorter than 100m. Since we want all of our segments in the grid to be around 200m, we have to merge these smaller segments to it's neighbour and make sure all segments are at least 200m.

Parameters:
- degree difference

    Degree difference between two neighbouring segments
- length

    The total length of neighbouring segments
- number of observations

    The total number of speed observations of that segment 


How it works:

In this example, we would want to merge Segment A to Segment B as it is too short. 
To merge Segment A to Segment B and *not* Segment C, we take the azimuth of both the end and the start of a segment, and calculate the degree difference of those segments to it's neighbouring segments. Knowing the degree difference between Segment A & Segment B, and Segment A & Segment C, we can make a sensible choice of merging the segments that has the lower degree difference. 

To order and select best two merging choice for each segment, we use this sql.

We then used this script using this logic for all segments until there are no avaliable segments for merging. (e.g. only neighbouring segment was used to merge with a segment that has a higher priority)


Future Improvements:
- Make merging script a loop
- Add another rule for segments with 2 ranks, allowing the consideration of 2nd choice if 1st choice was used for merging before


## Partition segments
### Greedy Partitioning

Add next link to a partition until the parition reaches 200m. If the last segment is shorter than 100m, add it to the previous partition.




### Best Group 

Produces groups of all possible combination of partitions, select the one partition with less error. Since this method is very computatively expensive, we only used it for segments that produces less than 15 million combinations. 

### Simulated Annealing

For segments that do not qualify for using the best group partitioning, they uses the simulated annealing method. Instead of finding all the possible combination, simulated annealing find the optimal "state" from a certain number of iterations. Check out this notebook for a more in depth explaination and examples.


