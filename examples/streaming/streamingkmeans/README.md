Streaming k-means clustering
==============================
## Introduction
This application is following Streaming k-means clustering on Spark, you can see for details at
<https://databricks.com/blog/2015/01/28/introducing-streaming-k-means-in-spark-1-2.html>. 

The DataSource used is `RandomRBFGenerator`, which is referenced by Huawei `StreamDM` <https://github.com/huawei-noah/streamDM>.

## Gearpump topology
The Gearpump topology is as following:

![kmeans](https://cloud.githubusercontent.com/assets/5796671/14097520/93a2b498-f5a4-11e5-8df8-ef2b62c3b5ff.PNG)

The `Source Processor` will produce points by time, then broadcast the point to the `Distribution Processor`.
The number of tasks of the `Distribution Processor` is k, where each task save one center and the corresponding points.
When `Distribution Processor` receives a point from `Source Processor`, it will calculate the distance of this point to its center, and then send the distance along with the point and its `taskId` to the `Collection Processor`.
When `Collection Processor` receives the distance from `Distribution Processor`, it will accumulate the number of current points, determine if it's time to update center, choose the smallest distance and then send the point along with its corresponding `Distribution Processor` taskId by broadcast partitioner.
When `Distribution Processor` receives the result message, task with the corresponding `taskId` will accumulate the point. If `Distribution Processor` receives that it's time to update center, then all the tasks will update its corresponding center.

This procedure is streaming and the center of cluster will change by time.

## How to use it
You can used this application by command:

```
bin/gear app -jar examples/streamingkmeans-2.11-0.7.7-SNAPSHOT-assembly.jar io.gearpump.streaming.examples.streamingkmeans.StreamingKmeansExample
```

As an option, you can configure the clustering task by the following command:

```
-k <how many clusters (k in kmeans)>
-dimension <dimension of a point>
-maxBatch <number of data a batch for DataSourceProcessor>
-maxNumber <number of data to do a clustering procedure>
-decayFactor <decay factor for clustering, used by updating center>
```

## Evaluation
The number of task of the `Distribution Processor` is k, where each task saves one cluster center.
It will output the cluster center once they have been updated.
