package net.sansa_stack.rdf.partition.spark.utils

import net.sansa_stack.rdf.partition.spark.utils.TripleGroupType.TripleGroupType
import org.apache.jena.graph.Node
import org.apache.spark.graphx._

/**
  * Construct triple groups for input vertices
  *
  * Subject-based triples groups: s-TG of vertex v∈V is a set of triples in which their subject is v
  * denoted by s-TG(v)= {(u,w)\(u,w)∈E, u = v}
  *
  * Object-based triples groups: o-TG of vertex v∈V is a set of triples in which their object is v
  * denoted by s-TG(v)= {(u,w)\(u,w)∈E, w = v}
  *
  * Subject-object-based triple groups: so-TG of vertex v∈V is a set of triples in which their object is v
  * denoted by s-TG(v)= {(u,w)\(u,w)∈E, v∈{u,w}}
  *
  * @author Zhe Wang
  */
class TripleGroup(graph: Graph[Node,Node], tgt:TripleGroupType) extends Serializable {

  graph.cache()
  val ops = graph.ops
  val direction = determineType()
  val verticesGroupSet = setVerticesGroupSet()
  val edgesGroupSet = setEdgesGroupSet()

  private def determineType(): Option[EdgeDirection] = {
    tgt match {
      case TripleGroupType.s => Some(EdgeDirection.Out)
      case TripleGroupType.o => Some(EdgeDirection.In)
      case TripleGroupType.so => Some(EdgeDirection.Either)
    }
  }

  private def setVerticesGroupSet(): VertexRDD[Array[(VertexId,Node)]] = {
    ops.collectNeighbors(direction.get)
  }

  private def setEdgesGroupSet(): VertexRDD[Array[Edge[Node]]] = {
    ops.collectEdges(direction.get)
  }
}

object TripleGroupType extends Enumeration{
  type TripleGroupType = Value
  val s, o, so = Value
}
