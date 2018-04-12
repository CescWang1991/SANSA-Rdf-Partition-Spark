package net.sansa_stack.rdf.query.graph.jena.expression

import org.apache.jena.graph.Node
import org.apache.jena.sparql.expr.{E_Regex, Expr, ExprFunctionN}
import org.apache.spark.rdd.RDD

/**
  * Class that evaluate solution based on expression. Support expression as FILTER regex(?user "tw:user0")
  * @param expr expression of regex
  */
class ExprRegex(expr: Expr) extends ExprFilter {
  override def evaluation(solutionMapping: RDD[Map[Node, Node]]): Boolean = {
    println(expr)
    true
  }
}
