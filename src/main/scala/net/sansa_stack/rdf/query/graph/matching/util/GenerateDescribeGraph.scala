package net.sansa_stack.rdf.query.graph.matching.util

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
  * Returns a single result RDF graph containing RDF data from the input solution mapping.
  */
object GenerateDescribeGraph {

  def run[VD: ClassTag, ED: ClassTag](mapping: Array[Map[VD, VD]], graph: Graph[VD, ED]): Graph[VD, ED] = {
    val attrSet = mapping.flatMap(m => m.valuesIterator)
    val validGraph = graph.subgraph(epred = edge => attrSet.contains(edge.srcAttr) || attrSet.contains(edge.dstAttr))
    validGraph
  }
}
