package net.sansa_stack.rdf.partition.spark.strategy

import org.apache.spark.graphx.{Graph, PartitionID, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

abstract class PartitionStrategy[VD: ClassTag,ED: ClassTag](
    val graph: Graph[VD,ED],
    val session: SparkSession,
    private var numPartitions: PartitionID) extends Serializable {

  def this(graph: Graph[VD,ED], session: SparkSession) = {
    this(graph, session, graph.edges.partitions.length)
  }

  /**
    * Repartition the edges in the graph and construct a new graph that partitioned by strategy
    * with input number of partitions.
    *
    */
  def partitionBy(): Graph[VD,ED]

  /**
    * Repartition the vertices in the graph according to partition strategy, vertices will be assigned
    * if they are source or destination of triples in specific partition
    *
    * @return RDD of vertices
    */
  def getVertices(): RDD[(VertexId,VD)]
}