package net.sansa_stack.rdf.partition.spark.utils

import org.apache.jena.graph.Node
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}

/**
  * Construct subject triple groups for input vertices
  * Subject triples groups: s-TG of vertex v∈V is a set of triples in which their subject is v
  * denoted by s-TG(v)= {(u,w)\(u,w)∈E, u = v}
  *
  * @author Zhe Wang
  */
object SubjectTripleGroup extends Serializable {
  /**
    * Constructs GraphX graph from loading a RDF file
    * @param vertices Input vertices
    * @param graph RDF Graph
    * @return List[(VertexId,Graph[Node,Node])].
    */
  def apply(vertices:VertexRDD[Node],graph: Graph[Node,Node]) : List[(VertexId,Graph[Node,Node])]= {
    graph.cache()
    val verticeslist = vertices.map{case(id,_)=>id}.collect.toList
    verticeslist.map(id=>(id,setTripleGroup(id,graph)))
  }

  def setTripleGroup(id: VertexId,graph: Graph[Node,Node]): Graph[Node,Node] = {
    val subgraphStepOne = graph.subgraph(epred = triplet=>triplet.srcId==id)
    val dstId = subgraphStepOne.edges.map(e=>e.dstId).collect()
    subgraphStepOne.subgraph(vpred = (vertexId,_)=>dstId.contains(vertexId)||vertexId==id)
  }
}
