package net.sansa_stack.rdf.partition.spark.utils

import org.apache.jena.graph.Node
import org.apache.spark.graphx.{Graph, VertexId}

/**
  * Construct subject-object-based triple groups for input vertices
  * Subject-object-based triple groups: so-TG of vertex v∈V is a set of triples in which their object is v
  * denoted by s-TG(v)= {(u,w)\(u,w)∈E, v∈{u,w}}
  *
  * @author Zhe Wang
  */
object SOTripleGroup extends Serializable with GenerateTripleGroup {

  def setTripleGroup(id: VertexId,graph: Graph[Node,Node]): Graph[Node,Node] = {
    val subgraphStepOne = graph.subgraph(epred = triplet=>triplet.dstId==id || triplet.srcId==id)
    val srcId = subgraphStepOne.edges.map(e=>e.srcId).collect()
    val dstId = subgraphStepOne.edges.map(e=>e.dstId).collect()
    subgraphStepOne.subgraph(vpred = (vertexId,_)=>srcId.contains(vertexId)||dstId.contains(vertexId))
  }
}
