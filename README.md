# SANSA-Rdf-Partition-Spark
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

SANSA RDF Graph Partitioning is a library to perform RDF Graph Partitioning algorithms by implementing SANSA_Stack framework(see http://sansa-stack.net). In the case, [RDF](https://en.wikipedia.org/wiki/Resource_Description_Framework) files will be transformed as [org.apache.spark.graphx.Graph](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.Graph) and the algorithms will be performed on [Apache Spark](https://spark.apache.org).

The SANSA-Rdf-Partition-Spark currently supports algorithms provided by following papers:
* [Semantic Hash Partitioning](https://dl.acm.org/citation.cfm?id=2556571): Kisung Lee and Ling Liu. Scaling queries over big rdf graphs with semantic hash partitioning. Proceedings of the VLDB Endowment, 6(14): 1894-1905, 2013. (Completed)
* [Path Partitioning](http://ieeexplore.ieee.org/abstract/document/7113334/): Buwen Wu, Yongluan Zhou, Pingpeng Yuan, Ling Liu, and Hai Jin. Scalable sparql querying using path partitioning. In Data Engineering (ICDE), 2015 IEEE 31st International Conference on, pages 795–806. IEEE, 2015. (In construction)

## Example#1: Semantic Hash Partitioning
Load [N-Triple file](https://github.com/CescWang1991/SANSA-Rdf-Partition-Spark/blob/master/src/main/resources/SilviaClustering_HairStylist_TaxiDriver.nt) to our example and apply a graph partitioning algorithm named Semantic Hash Partitioning. The example graph has in total 465 vertices and 516 edges, which is partitioned into 4 partitions. The number of vertices and edges in each partition are shown in following table.

| PartitionId | Vertices before | Vertices after 1-hop | Edges after 1-hop | Vertices after 2-hop | Edges after 2-hop | Vertices after 3-hop | Edges after 3-hop |
| :----- | :----- | :----- | :----- | :----- | :----- | :----- | :----- |
| 0 | 128 | 136 | 75 | 315 | 342 | 315 | 350 |
| 1 | 116 | 226 | 219 | 316 | 361 | 316 | 361 |
| 2 | 127 | 213 | 180 | 307 | 338 | 307 | 348 |
| 3 | 94 | 100 | 42 | 295 | 306 | 296 | 325 |

## Example#2: Path Partitioning
Load [N-Triple file](https://github.com/CescWang1991/SANSA-Rdf-Partition-Spark/blob/master/src/main/resources/Clustering_sampledata1.nt) as example rdf graph. The example graph is shown as follow:

<p align="center"> 
  <img src="https://github.com/CescWang1991/SANSA-Rdf-Partition-Spark/blob/master/src/main/resources/Path%20Partition-Example%20Graph.jpg">
</p>
