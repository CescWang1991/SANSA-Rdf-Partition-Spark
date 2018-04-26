package net.sansa_stack.rdf.query.graph.jena.expression

import org.apache.jena.graph.{Node, NodeFactory}
import org.apache.jena.sparql.expr.{E_Add, Expr, NodeValue}

/**
  * Class that evaluate solution based on expression. Support expression as FILTER (?age >= 18).
  * @param left Expression of left side.
  * @param right Expression of right side.
  *
  * @author Zhe Wang
  */
class ExprCompare(left: Expr, right: Expr) extends ExprFilter {

  private val tag = "Filter Comparision"
  private var comp = ""

  override def evaluate(solution: Map[Node, Node]): Boolean = {

    val leftValue: Node = solution(left.asVar().asNode())
    val rightValue: Node = {
      if(right.isConstant){
        right.getConstant.getNode
      }
      else if(right.isFunction) {
        right match {
          case e: E_Add => if(e.getArg1.isVariable) {
            NodeFactory.createLiteral((solution(e.getArg1.asVar.asNode).getLiteralValue.toString.toDouble +
              e.getArg2.getConstant.toString.toDouble).toString)

          } else {
            NodeFactory.createLiteral((e.getArg1.getConstant.toString.toDouble +
              solution(e.getArg2.asVar.asNode).getLiteralValue.toString.toDouble).toString)
          }
        }
      }
      else if(right.isVariable){
        solution(right.asVar().asNode())
      }
      else { null }
    }
    //println("right value: "+rightValue)

    if(leftValue.isLiteral) {    //Both sides are literals
      comp match {
        case "Equals" =>
          leftValue.getLiteralValue.toString.toDouble == rightValue.getLiteralValue.toString.toDouble
        case "Not Equals" =>
          leftValue.getLiteralValue.toString.toDouble != rightValue.getLiteralValue.toString.toDouble
        case "Greater Than" =>
          leftValue.getLiteralValue.toString.toDouble > rightValue.getLiteralValue.toString.toDouble
        case "Greater Than Or Equal" =>
          leftValue.getLiteralValue.toString.toDouble >= rightValue.getLiteralValue.toString.toDouble
        case "Less Than" =>
          leftValue.getLiteralValue.toString.toDouble < rightValue.getLiteralValue.toString.toDouble
        case "Less Than Or Equal" =>
          leftValue.getLiteralValue.toString.toDouble <= rightValue.getLiteralValue.toString.toDouble
        case _ => true
      }
    }
    else if(leftValue.isURI) {    //Both sides are URIs
      comp match {
        case "Equals" => leftValue.getURI.equals(rightValue.toString)
        case "Not Equals" => !leftValue.getURI.equals(rightValue.toString)
        case _ => true
      }
    }
    else if(right.isVariable) {     //Right side is variable
      true
    }
    else {
      true
    }
  }

  override def getTag: String = { tag }

  def setComp(comp: String): ExprCompare = {
    this.comp = comp
    this
  }
}
