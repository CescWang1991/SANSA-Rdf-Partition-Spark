package net.sansa_stack.rdf.query.graph.jena.exprFilter

import org.apache.jena.graph.Node

trait ExprFilter extends Serializable {

  def evaluate(solution: Map[Node, Node]): Boolean

  def getTag: String

}
