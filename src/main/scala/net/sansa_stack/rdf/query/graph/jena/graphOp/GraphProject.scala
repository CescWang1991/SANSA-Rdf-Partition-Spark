package net.sansa_stack.rdf.query.graph.jena.graphOp

import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.op.OpProject
import org.apache.jena.sparql.core.Var

import scala.collection.JavaConversions._

/**
  * Class that execute the operations of projecting the required variables.
  * @param op Project operator.
  */
class GraphProject(val op: OpProject) extends GraphOp {

  private val tag = "SELECT"
  private val vars = op.getVars.toList

  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    input.map{ mapping =>
      mapping.filter{ case(k,_) => vars.contains(k) }
    }
  }

  def test(): Unit = {
    println(op)
    println(vars)
  }

  override def getTag: String = { tag }

  def getVars: List[Var] = { vars }
}
