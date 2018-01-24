package net.sansa_stack.rdf.partition.spark.utils

import org.apache.jena.graph.Node
import org.apache.spark.graphx.{Graph, VertexId}

/**
  * Construct object-based triple groups for input vertices
  * Object-based triples groups: o-TG of vertex v∈V is a set of triples in which their object is v
  * denoted by s-TG(v)= {(u,w)\(u,w)∈E, w = v}
  *
  * @author Zhe Wang
  */
object ObjectTripleGroup extends Serializable with GenerateTripleGroup {

  def setTripleGroup(id: VertexId,graph: Graph[Node,Node]): Graph[Node,Node] = {
    val subgraphStepOne = graph.subgraph(epred = triplet=>triplet.dstId==id)
    val srcId = subgraphStepOne.edges.map(e=>e.srcId).collect()
    subgraphStepOne.subgraph(vpred = (vertexId,_)=>srcId.contains(vertexId)||vertexId==id)
  }
}
