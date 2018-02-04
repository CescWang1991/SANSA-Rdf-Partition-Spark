package net.sansa_stack.rdf.partition.spark.utils

import org.apache.jena.graph.Node
import org.apache.spark.graphx.{Graph, PartitionStrategy}

object BaselineHashPartitions {
  def apply(graph: Graph[Node,Node], ps: PartitionStrategy): Graph[Node,Node] = {
    graph.partitionBy(ps)
  }
}
