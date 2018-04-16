package net.sansa_stack.rdf.query.graph.jena.graphOp

import net.sansa_stack.rdf.query.graph.jena.ExprParser
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.op.OpFilter
import org.apache.jena.sparql.expr.ExprList
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
  * Class that execute SPARQL FILTER operation
  * @param op FILTER operation
  */
class GraphFilter(val op: OpFilter) extends GraphOp with Serializable {

  private val tag = "FILTER"
  private val expr = op.getExprs

  /**
    * Filter the result by the given filter expression
    * @param input solution mapping to be filtered
    * @return solution mapping after filtering
    */
  override def execute(input: RDD[Map[Node, Node]]): RDD[Map[Node, Node]] = {
    val exprParser = new ExprParser(expr)
    exprParser.exprVisitorWalker()
    val filterGroup = exprParser.getFilterGroup
    var intermediate = input
    filterGroup.foreach(exprFilter =>
      intermediate = intermediate.filter(solution => exprFilter.evaluate(solution)))
    val output = intermediate
    output
  }

  override def getTag: String = { tag }

  override def getExpr: ExprList = { expr }
}
