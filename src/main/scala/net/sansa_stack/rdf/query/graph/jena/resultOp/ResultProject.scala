package net.sansa_stack.rdf.query.graph.jena.resultOp

import net.sansa_stack.rdf.query.graph.matching.util.Result
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.op.OpProject
import org.apache.jena.sparql.core.Var
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

/**
  * Class that execute the operations of projecting the required variables.
  * @param op Project operator.
  */
class ResultProject(val op: OpProject) extends ResultOp {

  private val tag = "SELECT"
  private val vars = op.getVars.toList
  private val varSet = vars.map(v => v.asNode()).toSet

  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    input.map{ mapping =>
      mapping.filter{ case(k,_) => vars.contains(k) }
    }
  }

  def test(input: RDD[Result[Node]], session: SparkSession): RDD[Result[Node]] = {
    val set = session.sparkContext.broadcast(varSet)
    input.map(result => result.project(set.value))
  }

  override def getTag: String = { tag }

  def getVars: List[Var] = { vars }
}
