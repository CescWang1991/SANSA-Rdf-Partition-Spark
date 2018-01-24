package net.sansa_stack.rdf.partition.spark.utils

import net.sansa_stack.rdf.partition.spark.utils.ObjectTripleGroup.setTripleGroup
import org.apache.jena.graph.Node
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}

trait GenerateTripleGroup {
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

  def setTripleGroup(id: VertexId,graph: Graph[Node,Node]): Graph[Node,Node]
}
