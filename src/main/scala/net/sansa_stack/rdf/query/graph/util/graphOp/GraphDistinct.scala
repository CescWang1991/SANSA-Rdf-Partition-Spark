package net.sansa_stack.rdf.query.graph.util.graphOp

import org.apache.jena.sparql.algebra.op.OpDistinct

class GraphDistinct(val op: OpDistinct) extends GraphOp {

  private val tag = "DISTINCT"

  override def execute(): Unit = {

  }

  override def getTag: String = { tag }
}
