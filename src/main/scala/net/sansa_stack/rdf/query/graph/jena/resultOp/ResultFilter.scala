package net.sansa_stack.rdf.query.graph.jena.resultOp

import net.sansa_stack.rdf.query.graph.jena.ExprParser
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.op.OpFilter
import org.apache.jena.sparql.expr.{Expr, ExprList}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
  * Class that execute SPARQL FILTER operation
  * @param expr FILTER expression
  */
class ResultFilter(val expr: Expr) extends ResultOp {

  private val tag = "FILTER"

  /**
    * Filter the result by the given filter expression
    * @param input solution mapping to be filtered
    * @return solution mapping after filtering
    */
  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    val exprParser = new ExprParser(expr)
    val filterOp = exprParser.getFilter
    var intermediate = input
    //filter.foreach(expr => intermediate = intermediate.filter(solution => expr.evaluate(solution)))
    intermediate = intermediate.filter(solution => filterOp.evaluate(solution))
    val output = intermediate
    output
  }

  override def getTag: String = { tag }

  def getExpr: Expr = { expr }
}
