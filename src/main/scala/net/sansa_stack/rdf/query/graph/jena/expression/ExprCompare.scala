package net.sansa_stack.rdf.query.graph.jena.expression

import org.apache.jena.graph.Node
import org.apache.jena.sparql.expr.{E_GreaterThan, E_GreaterThanOrEqual, ExprFunction2}

/**
  * Class that evaluate solution based on expression. Support expression as FILTER (?age > 18)
  * @param variable expression of variable
  * @param value expression of value
  */
class ExprCompare(variable: Node, value: Node, op: String) extends ExprFilter {

  private val tag = "Filter Comparision"

  override def evaluate(solution: Map[Node, Node]): Boolean = {
    if(solution(variable) == null){ false }
    else {
      op match {
        case "Equals" =>
          solution(variable).getLiteralValue.toString.toDouble == value.getLiteralValue.toString.toDouble
        case "Greater Than" =>
          solution(variable).getLiteralValue.toString.toDouble > value.getLiteralValue.toString.toDouble
        case "Greater Than Or Equal" =>
          solution(variable).getLiteralValue.toString.toDouble >= value.getLiteralValue.toString.toDouble
        case "Less Than" =>
          solution(variable).getLiteralValue.toString.toDouble < value.getLiteralValue.toString.toDouble
        case "Less Than Or Equal" =>
          solution(variable).getLiteralValue.toString.toDouble <= value.getLiteralValue.toString.toDouble
        case _ => false
      }
    }
  }

  override def getTag: String = { tag }
}
