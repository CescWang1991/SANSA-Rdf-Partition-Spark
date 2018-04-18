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

  def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]]

  def getTag: String
}
