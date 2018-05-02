package net.sansa_stack.rdf.query.graph.jena.exprFilter

import org.apache.jena.graph.Node
import org.apache.jena.sparql.expr.{E_Function, ExprVar, NodeValue}

import scala.collection.JavaConversions._

class ExprFunc(exprVar: ExprVar) {

  private var func: E_Function = _

  def setFunction(expr: E_Function): Unit = {
    this.func = expr
  }

  def evaluate(solution: Map[Node, Node]): Node = {
    if(func == null) {
      solution(exprVar.getAsNode)
    }
    else {
      solution(exprVar.getAsNode)
    }
  }
}
