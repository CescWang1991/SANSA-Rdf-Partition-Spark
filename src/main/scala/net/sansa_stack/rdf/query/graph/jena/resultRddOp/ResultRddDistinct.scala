package net.sansa_stack.rdf.query.graph.jena.resultRddOp

import net.sansa_stack.rdf.query.graph.matching.util.Result
import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class ResultRddDistinct extends ResultRddOp {

  private val tag = "DISTINCT"

  override def execute(input: RDD[Result[Node]], session: SparkSession): RDD[Result[Node]] = {
    input.distinct()
  }

  override def getTag: String = { tag }
}
