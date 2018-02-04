package net.sansa_stack.rdf.partition.spark.utils

import org.apache.jena.graph.Node
import org.apache.spark.graphx.{Graph, PartitionStrategy}

/**
  * Divide the RDF Graph into a set baseline hash partitions {p(1), p(2), ..., p(n)}
  * All vertices firstly are divided into disjoint subsets
  * Baseline hash partitions p(i) has vertices set v(i), 1≤i≤n
  * v(1)∪v(2)∪...∪v(n) = V, v(i)∩v(j) = ∅ (1≤i,j≤n)
  *
  * @author Zhe Wang
  */
object BaselineHashPartitions {

  def apply(graph: Graph[Node,Node], ps: PartitionStrategy): Graph[Node,Node] = {
    graph.partitionBy(ps)
  }
}
