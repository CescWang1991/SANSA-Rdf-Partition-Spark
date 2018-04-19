package net.sansa_stack.rdf.query.graph.jena.graphOp
import org.apache.jena.graph.Node

class GraphDistinct extends GraphOp {

  private val tag = "DISTINCT"

  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    input.distinct
  }

  override def getTag: String = { tag }
}
