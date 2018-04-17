package net.sansa_stack.rdf.query.graph.jena.expression

import org.apache.jena.graph.Node

/**
  * Class that evaluate solution based on expression. Support expression as FILTER regex(?user "tw:user0")
  * @param variable expression of variable
  * @param value expression of value
  */
class ExprRegex(variable: Node, value: Node) extends ExprFilter {

  private val tag = "Filter Regex"

  override def evaluate(solution: Map[Node, Node]): Boolean = {
    solution(variable).toString.contains(value.toString)
  }

  override def getTag: String = { tag }
}
