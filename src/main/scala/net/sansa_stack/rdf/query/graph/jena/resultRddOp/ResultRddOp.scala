package net.sansa_stack.rdf.query.graph.jena.resultRddOp

import net.sansa_stack.rdf.query.graph.jena.Ops
import net.sansa_stack.rdf.query.graph.matching.util.Result
import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Trait for all operations related to deal with solution mapping directly.
  *
  * @author Zhe Wang
  */
trait ResultRddOp extends Ops{

  def execute(input: RDD[Result[Node]], session: SparkSession): RDD[Result[Node]]

  override def getTag: String
}
