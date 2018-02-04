# SANSA-Rdf-Partition-Spark

SANSA RDF Graph Partitioning is a library to perform RDF Graph Partitioning algorithms by implementing SANSA_Stack framework(see http://sansa-stack.net). In the case, [RDF](https://en.wikipedia.org/wiki/Resource_Description_Framework) files will be transformed as [org.apache.spark.graphx.Graph](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.Graph) and the algorithms will be performed on [Apache Spark](https://spark.apache.org).

The SANSA-Rdf-Partition-Spark currently supports algorithms provided by following papers:
* [Semantic Hash Partitioning](https://dl.acm.org/citation.cfm?id=2556571): Kisung Lee and Ling Liu. Scaling queries over big rdf graphs with semantic hash partitioning. Proceedings of the VLDB Endowment, 6(14): 1894-1905, 2013. (Completed)
* [Path Partitioning](http://ieeexplore.ieee.org/abstract/document/7113334/): Buwen Wu, Yongluan Zhou, Pingpeng Yuan, Ling Liu, and Hai Jin. Scalable sparql querying using path partitioning. In Data Engineering (ICDE), 2015 IEEE 31st International Conference on, pages 795–806. IEEE, 2015. (In construction)
