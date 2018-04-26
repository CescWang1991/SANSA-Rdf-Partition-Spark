package net.sansa_stack.rdf.query.graph.jena

import net.sansa_stack.rdf.query.graph.jena.expression.{ExprBound, ExprCompare, ExprFilter, ExprRegex}
import org.apache.jena.graph.{Node, NodeFactory}
import org.apache.jena.sparql.algebra.op.OpBGP
import org.apache.jena.sparql.algebra.walker.{ExprVisitorFunction, Walker}
import org.apache.jena.sparql.expr._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable

class ExprParser(expr: Expr) extends ExprVisitorFunction with Serializable {

  private var filter = new mutable.Queue[ExprFilter]()
  private var left: Node = _
  private var right: Node = _
  private var value: Node = _

  Walker.walk(expr, this)

  override def visitExprFunction(func: ExprFunction): Unit = {
    println(func+":ExprFunction")
  }

  override def visit(func: ExprFunction0): Unit = {
    println(func+":ExprFunction0")
  }

  override def visit(func: ExprFunction1): Unit = {
    println(func+":ExprFunction1")
    func match {
      case _: E_Bound => filter += new ExprBound(left)
      case _: E_LogicalNot => filter.last.asInstanceOf[ExprBound].setLogic(false)
    }
  }

  override def visit(func: ExprFunction2): Unit = {
    println(func+":ExprFunction2")
    func match {
      case _: E_Equals =>
        filter += new ExprCompare(func.getArg1, func.getArg2).setComp("Equals")
      case _: E_NotEquals =>
        filter += new ExprCompare(func.getArg1, func.getArg2).setComp("Not Equals")
      case _: E_GreaterThan =>
        filter += new ExprCompare(func.getArg1, func.getArg2).setComp("Greater Than")
      case _: E_GreaterThanOrEqual =>
        filter += new ExprCompare(func.getArg1, func.getArg2).setComp("Greater Than Or Equal")
      case _: E_LessThan =>
        filter += new ExprCompare(func.getArg1, func.getArg2).setComp("Less Than")
      case _: E_LessThanOrEqual =>
        filter += new ExprCompare(func.getArg1, func.getArg2).setComp("Less Than Or Equal")
      case _: E_Add =>
      case _: E_Subtract => println(func+":E_Subtract")
      case _: E_LogicalAnd => println(func+":E_LogicalAnd")
      case _ =>
        throw new UnsupportedOperationException("Not support the expression of ExprFunction2")
    }
  }

  override def visit(func: ExprFunction3): Unit = {
    println(func+":ExprFunction3")
  }

  override def visit(func: ExprFunctionN): Unit = {
    println(func+":ExprFunctionN")
    func match {
      case _: E_Regex => filter += new ExprRegex(left, value)
      case _ =>  throw new UnsupportedOperationException("Not support the expression of ExprFunctionN")
    }
  }

  override def visit(exprFunctionOp: ExprFunctionOp): Unit = {
    println(exprFunctionOp+":ExprFunctionOp")
    exprFunctionOp match {
      case e: E_Exists => println(e.getElement)
    }
  }

  override def visit(exprAggregator: ExprAggregator): Unit = {
    println(exprAggregator+":ExprAggregator")
  }

  override def visit(exprNone: ExprNone): Unit = {
    println(exprNone+":ExprNone")
  }

  override def visit(exprVar: ExprVar): Unit = {
    println(exprVar+":ExprVar")
    if(left == null){
      left= exprVar.getAsNode
    } else {
      right = exprVar.getAsNode
    }
  }

  override def visit(nodeValue: NodeValue): Unit = {
    println(nodeValue+":NodeValue")
    value = nodeValue.asNode()
  }

  def getFilter: mutable.Queue[ExprFilter] = {
    filter
  }
}
