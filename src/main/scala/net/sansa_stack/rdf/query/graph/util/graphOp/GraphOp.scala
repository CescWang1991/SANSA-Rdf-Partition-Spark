package net.sansa_stack.rdf.query.graph.util.graphOp

/**
  * Trait for all graph operators
  *
  * @author Zhe Wang
  */
trait GraphOp {

  def execute(): Unit

  def getTag: String
}
