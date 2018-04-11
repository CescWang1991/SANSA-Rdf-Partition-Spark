package net.sansa_stack.rdf.query.graph.util.graphOp

import org.apache.jena.sparql.algebra.op.OpFilter
import scala.collection.JavaConversions._

class GraphFilter(val op: OpFilter) extends GraphOp {

  private val tag = "FILTER"

  override def execute(): Unit = {
    println(op.getExprs)
  }

  override def getTag: String = { tag }
}
