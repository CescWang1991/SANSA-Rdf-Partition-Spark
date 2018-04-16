package net.sansa_stack.rdf.query.graph.jena.graphOp

import org.apache.jena.graph.Node
import org.apache.jena.sparql.expr.ExprList
import org.apache.spark.rdd.RDD

/**
  * Trait for all graph operators
  *
  * @author Zhe Wang
  */
trait GraphOp extends Serializable {

  def execute(input: RDD[Map[Node, Node]]): RDD[Map[Node, Node]]

  def getTag: String

  def getExpr: ExprList
}
