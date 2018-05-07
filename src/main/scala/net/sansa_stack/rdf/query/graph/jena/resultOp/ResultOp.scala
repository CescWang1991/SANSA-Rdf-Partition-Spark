package net.sansa_stack.rdf.query.graph.jena.resultOp

import net.sansa_stack.rdf.query.graph.jena.Ops
import org.apache.jena.graph.Node

/**
  * Trait for all operations related to deal with solution mapping directly.
  *
  * @author Zhe Wang
  */
trait ResultOp extends Ops {

  def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]]

  override def getTag: String
}
