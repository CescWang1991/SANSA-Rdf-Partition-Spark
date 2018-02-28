# SANSA-Rdf-Partition-Spark
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

SANSA RDF Graph Partitioning is a library to perform RDF Graph Partitioning algorithms by implementing SANSA_Stack framework(see http://sansa-stack.net). In the case, [RDF](https://en.wikipedia.org/wiki/Resource_Description_Framework) files will be transformed as [org.apache.spark.graphx.Graph](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.Graph) and the algorithms will be performed on [Apache Spark](https://spark.apache.org).

The SANSA-Rdf-Partition-Spark currently supports algorithms provided by following papers:
* [Semantic Hash Partitioning](https://dl.acm.org/citation.cfm?id=2556571): Kisung Lee and Ling Liu. Scaling queries over big rdf graphs with semantic hash partitioning. Proceedings of the VLDB Endowment, 6(14): 1894-1905, 2013. (Completed)
* [Path Partitioning](http://ieeexplore.ieee.org/abstract/document/7113334/): Buwen Wu, Yongluan Zhou, Pingpeng Yuan, Ling Liu, and Hai Jin. Scalable sparql querying using path partitioning. In Data Engineering (ICDE), 2015 IEEE 31st International Conference on, pages 795–806. IEEE, 2015. (In construction)

## Example#1: Semantic Hash Partitioning
Load [N-Triple file](https://github.com/CescWang1991/SANSA-Rdf-Partition-Spark/blob/master/src/main/resources/SilviaClustering_HairStylist_TaxiDriver.nt) to our example and apply a graph partitioning algorithm named Semantic Hash Partitioning. The example graph has in total 465 vertices and 499 edges, which is partitioned into 4 partitions. Firstly, we apply sementic hash partition using subject triple groups. The number of vertices and edges in each partition are shown in following table.

| PartitionId | Edges after 1-hop | Edges after 2-hop | Edges after 3-hop |
| :----- | :----- | :----- | :----- |
| 0 | 71 | 331 | 339
| 1 | 212 | 344 | 344
| 2 | 174 | 323 | 331
| 3 | 42 | 301 | 316

Then we apply sementic hash partition by using object triple groups:

| PartitionId | Edges after 1-hop | Edges after 2-hop | Edges after 3-hop |
| :----- | :----- | :----- | :----- |
| 0 | 73 | 309 | 319 |
| 1 | 261 | 307 | 307 |
| 2 | 109 | 317 | 330 |
| 3 | 56 | 297 | 312 |

Finally, we apply semantic hash partition by using subject-object(so) triple groups:

| PartitionId | Edges after 1-hop | Edges after 2-hop | Edges after 3-hop |
| :----- | :----- | :----- | :----- |
| 0 | 71 | 496 | 499 |
| 1 | 212 | 499 | 499 |
| 2 | 174 | 499 | 499 |
| 3 | 42 | 493 | 499 |

Semantic hash partition is suitable for star queries, however, partition applied by subject triple group can only ensure star queries whose core in the star has only outgoing edges, and same for partition applied by object triple group. Although partition applied by so triple group can solve the problem, in previous example we can find after 2-hop-expansion, the size of graph in each partition is close to original graph.

## Example#2: Path Partitioning
Load [N-Triple file](https://github.com/CescWang1991/SANSA-Rdf-Partition-Spark/blob/master/src/main/resources/Clustering_sampledata1.nt) as example rdf graph. The example graph is shown as follow:

<p align="center"> 
  <img src="https://github.com/CescWang1991/SANSA-Rdf-Partition-Spark/blob/master/Figures/Graph%20for%20PP.jpg">
</p>

For Path Partitioning, it exacts end to end paths and generate path groups for each start vertices (Vertices has only outgoing edges, no incoming edges). Then merges a vertex according to number of paths pass through this vertex. In the example, start vertices are {1,3,8,9}, if we want to has 2 partitions, then paths from {1,8} are merged, paths from {3,9} are merged. So we have two partitions as follow:

<p align="center"> 
  <img src="https://github.com/CescWang1991/SANSA-Rdf-Partition-Spark/blob/master/Figures/Path%20Partition.jpg">
</p>

Same graph partitioned by semantic hash partition with 2 partitions:

<p align="center"> 
  <img src="https://github.com/CescWang1991/SANSA-Rdf-Partition-Spark/blob/master/Figures/Semantic%20Hash%20Partition.jpg">
</p>

Remark: Currently path partition algorithm is unable to handle with graphs which has paths contain directed circle.
