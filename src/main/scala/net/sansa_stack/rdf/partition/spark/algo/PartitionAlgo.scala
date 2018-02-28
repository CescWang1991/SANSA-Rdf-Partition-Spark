package net.sansa_stack.rdf.partition.spark.algo

import org.apache.spark.graphx.{Graph, PartitionID, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

abstract class PartitionAlgo[VD: ClassTag,ED: ClassTag](
    val graph: Graph[VD,ED],
    val session: SparkSession,
    val numPartitions: PartitionID) extends Serializable {

  def this(graph: Graph[VD,ED], session: SparkSession) = {
    this(graph, session, graph.edges.partitions.length)
  }

  /**
    * Repartition the edges in the graph and construct a new graph that partitioned by strategy
    * with input number of partitions.
    *
    */
  def partitionBy(): Graph[VD,ED]
}