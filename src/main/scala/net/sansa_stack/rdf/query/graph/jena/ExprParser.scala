package net.sansa_stack.rdf.query.graph.jena

import net.sansa_stack.rdf.query.graph.jena.expression.{ExprFilter, ExprRegex}
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.walker.{ExprVisitorFunction, Walker}
import org.apache.jena.sparql.expr._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable

class ExprParser(exprs: ExprList) extends ExprVisitorFunction with Serializable {

  private var exprFilterGroup = mutable.Queue[ExprFilter]()
  private val left = new mutable.Stack[Node]
  private val right = new mutable.Stack[Node]

  def exprVisitorWalker(): Unit = {
    Walker.walk(exprs, this)
  }

  override def visitExprFunction(func: ExprFunction): Unit = {
    println(func+":ExprFunction")
  }

  override def visit(func: ExprFunction0): Unit = {
    println(func+":ExprFunction0")
  }

  override def visit(func: ExprFunction1): Unit = {
    println(func+":ExprFunction1")
  }

  override def visit(func: ExprFunction2): Unit = {
    println(func+":ExprFunction2")
  }

  override def visit(func: ExprFunction3): Unit = {
    println(func+":ExprFunction3")
  }

  override def visit(func: ExprFunctionN): Unit = {
    func match {
      case e: E_Regex => println(e+":ExprFunctionN")
        exprFilterGroup += new ExprRegex(left.pop(), right.pop())

      case _ =>  throw new UnsupportedOperationException("Not support the expression of ExprFunctionN")
    }
  }

  override def visit(exprFunctionOp: ExprFunctionOp): Unit = {
    println(exprFunctionOp+":ExprFunctionOp")
  }

  override def visit(exprAggregator: ExprAggregator): Unit = {
    println(exprAggregator+":ExprAggregator")
  }

  override def visit(exprNone: ExprNone): Unit = {
    println(exprNone+":ExprNone")
  }

  override def visit(exprVar: ExprVar): Unit = {
    println(exprVar+":ExprVar")
    left.push(exprVar.getAsNode)
  }

  override def visit(nodeValue: NodeValue): Unit = {
    println(nodeValue+":NodeValue")
    right.push(nodeValue.asNode())
  }

  def getFilterGroup: mutable.Queue[ExprFilter] = {
    exprFilterGroup
  }
}
