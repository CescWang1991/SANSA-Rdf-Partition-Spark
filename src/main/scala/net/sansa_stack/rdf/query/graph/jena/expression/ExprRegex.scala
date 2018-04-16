package net.sansa_stack.rdf.query.graph.jena.expression

import org.apache.jena.graph.Node
import org.apache.jena.sparql.expr._
import org.apache.spark.rdd.RDD

/**
  * Class that evaluate solution based on expression. Support expression as FILTER regex(?user "tw:user0")
  * @param variable expression of variable
  * @param value expression of value
  */
//class ExprRegex(expr: Expr) extends ExprFilter {
class ExprRegex(variable: Node, value: Node) extends ExprFilter with Serializable {

  private val tag = "Filter Regex"

  override def evaluate(solution: Map[Node, Node]): Boolean = {
    solution(variable).toString.contains(value.toString)
  }

  override def getTag: String = { tag }
}
