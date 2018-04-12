package net.sansa_stack.rdf.query.graph.jena.graphOp

import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.Op
import org.apache.jena.sparql.algebra.op.OpFilter
import org.apache.jena.sparql.expr.ExprList
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
  * Class that execute SPARQL FILTER operation
  * @param op FILTER operation
  */
class GraphFilter(val op: OpFilter) extends GraphOp {

  private val tag = "FILTER"
  private val exprs = op.getExprs.toIterator

  override def execute(): Unit = {
    exprs.foreach(expr => println(expr))
  }

  /**
    * Filter the result by the given filter expression
    * @param input solution mapping to be filtered
    * @return solution mapping after filtering
    */
  def execute(input: RDD[Map[Node, Node]]) = {
    //exprs.foreach(expr => println(expr))
  }

  override def getTag: String = { tag }

  override def getExpr: ExprList = { op.getExprs }
}
