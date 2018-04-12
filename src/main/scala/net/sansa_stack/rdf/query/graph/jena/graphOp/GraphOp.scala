package net.sansa_stack.rdf.query.graph.jena.graphOp

import org.apache.jena.sparql.algebra.Op
import org.apache.jena.sparql.expr.ExprList

/**
  * Trait for all graph operators
  *
  * @author Zhe Wang
  */
trait GraphOp {

  def execute(): Unit

  def getTag: String

  def getExpr: ExprList
}
