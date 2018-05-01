package net.sansa_stack.rdf.query.graph.jena.exprFilter

import javax.xml.datatype.XMLGregorianCalendar
import org.apache.jena.datatypes.RDFDatatype
import org.apache.jena.graph.{Node, NodeFactory}
import org.apache.jena.sparql.expr._

/**
  * Class that evaluate solution based on expression. Support expression as FILTER (?age >= 18).
  * @param left Expression of left side.
  * @param right Expression of right side.
  *
  * @author Zhe Wang
  */
class ExprCompare(left: Expr, right: Expr, e: ExprFunction2) extends ExprFilter {

  private val tag = "Filter Comparision"

  override def evaluate(solution: Map[Node, Node]): Boolean = {

    val leftValue = solution(left.asVar().asNode())
    val boolean: Boolean = {
      if(right.isConstant){
        compare(leftValue, right)
      }
      else if(right.isFunction) {
        right match {
          case e: E_Add => if(e.getArg1.isVariable) {
            compare(leftValue, NodeValue.makeDouble(solution(e.getArg1.asVar.asNode).getLiteralValue.toString.toDouble +
              e.getArg2.getConstant.toString.toDouble))
          } else {
            compare(leftValue, NodeValue.makeDouble(e.getArg1.getConstant.toString.toDouble +
              solution(e.getArg2.asVar.asNode).getLiteralValue.toString.toDouble))
          }
          case e: E_Subtract => if(e.getArg1.isVariable) {
            compare(leftValue, NodeValue.makeDouble(solution(e.getArg1.asVar.asNode).getLiteralValue.toString.toDouble -
              e.getArg2.getConstant.toString.toDouble))
          } else {
            compare(leftValue, NodeValue.makeDouble(e.getArg1.getConstant.toString.toDouble -
              solution(e.getArg2.asVar.asNode).getLiteralValue.toString.toDouble))
          }
        }
      }
      else if(right.isVariable){
        solution(right.asVar().asNode())
        false
      }
      else { false }
    }

    boolean
  }

  override def getTag: String = { tag }

  private def compare(leftValue: Node, right: Expr): Boolean = {
    if(right.getConstant.isDate){   //compare date
      e.eval(NodeValue.makeDate(leftValue.getLiteralLexicalForm),
        NodeValue.makeDate(right.getConstant.getDateTime)).toString.equals("true")
    }
    else if(right.getConstant.isInteger){   //compare integer
      e.eval(NodeValue.makeInteger(leftValue.getLiteralLexicalForm),
        NodeValue.makeInteger(right.getConstant.getInteger)).toString.equals("true")
    }
    else if(right.getConstant.isFloat) {    //compare float
      e.eval(NodeValue.makeFloat(leftValue.getLiteralLexicalForm.toFloat),
        NodeValue.makeFloat(right.getConstant.getFloat)).toString.equals("true")
    }
    else if(right.getConstant.isDouble) {   //compare double
      e.eval(NodeValue.makeDouble(leftValue.getLiteralLexicalForm.toDouble),
        NodeValue.makeDouble(right.getConstant.getDouble)).toString.equals("true")
    }
    else if(right.getConstant.isIRI) {    //compare URI
      e.eval(NodeValue.makeNode(leftValue), NodeValue.makeNode(right.getConstant.asNode)).toString.equals("true")
    }
    else{
      false
    }
  }
}
