package net.sansa_stack.rdf.query.graph.util.graphOp

import org.apache.jena.sparql.algebra.op.OpUnion

class GraphUnion(op: OpUnion) extends GraphOp {

  private val tag = "UNION"

  override def execute(): Unit = {

  }

  override def getTag: String = { tag }
}
