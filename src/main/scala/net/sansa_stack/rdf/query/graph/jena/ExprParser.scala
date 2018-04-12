package net.sansa_stack.rdf.query.graph.jena

import net.sansa_stack.rdf.query.graph.jena.expression.{ExprFilter, ExprRegex}
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.walker.{ExprVisitorFunction, Walker}
import org.apache.jena.sparql.expr._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable

class ExprParser(exprs: ExprList) extends ExprVisitorFunction {

  private var exprFilterGroup = mutable.Queue[ExprFilter]()
  var solutionMapping: RDD[Map[Node, Node]] = _

  def exprVisitorWalker(): Unit = {
    Walker.walk(exprs, this)
  }

  override def visitExprFunction(func: ExprFunction): Unit = {

  }

  override def visit(func: ExprFunction0): Unit = {

  }

  override def visit(func: ExprFunction1): Unit = {

  }

  override def visit(func: ExprFunction2): Unit = {

  }

  override def visit(func: ExprFunction3): Unit = {

  }

  override def visit(func: ExprFunctionN): Unit = {
    if(func.isInstanceOf[E_Regex]) {
      new ExprRegex(func.getExpr).evaluation(solutionMapping)
    }
    else{
      throw new UnsupportedOperationException("Not support the expression of ExprFunctionN")
    }
  }

  override def visit(exprFunctionOp: ExprFunctionOp): Unit = {

  }

  override def visit(exprAggregator: ExprAggregator): Unit = {

  }

  override def visit(exprNone: ExprNone): Unit = {

  }

  override def visit(exprVar: ExprVar): Unit = {

  }

  override def visit(nodeValue: NodeValue): Unit = {

  }
}
