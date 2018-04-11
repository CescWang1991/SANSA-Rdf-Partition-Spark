package net.sansa_stack.rdf.query.graph.util.graphOp

import org.apache.jena.sparql.algebra.op.OpOrder

class GraphOrder(op: OpOrder) extends GraphOp {

  private val tag = "ORDER BY"

  override def execute(): Unit = {

  }

  override def getTag: String = { tag }
}
