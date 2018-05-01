package net.sansa_stack.rdf.query.graph.jena.newOp

import net.sansa_stack.rdf.query.graph.matching.util.Result
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.op.OpProject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

class NewProject(val op: OpProject) extends NewOp {
  private val tag = "SELECT"
  private val varSet = op.getVars.toList.map(v => v.asNode()).toSet

  override def execute(input: RDD[Result[Node]], session: SparkSession): RDD[Result[Node]] = {
    val broadcast = session.sparkContext.broadcast(varSet)
    input.map(result => result.project(broadcast.value))
  }

  override def getTag: String = { tag }
}
