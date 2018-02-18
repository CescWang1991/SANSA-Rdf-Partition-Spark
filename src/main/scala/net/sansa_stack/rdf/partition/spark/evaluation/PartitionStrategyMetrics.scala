package net.sansa_stack.rdf.partition.spark.evaluation

import net.sansa_stack.rdf.partition.spark.strategy.PartitionStrategy
import org.apache.spark.graphx.{Graph, PartitionID}

import scala.reflect.ClassTag


/**
  * Evaluator for partition strategy.
  *
  * @param ps Partition Strategy
  * @param numPartitions number of partitions
  *
  * @tparam VD the vertex attribute associated with each vertex in the set.
  * @tparam ED the edge attribute associated with each edge in the set.
  */
class PartitionStrategyMetrics[VD: ClassTag, ED: ClassTag](
    ps:PartitionStrategy[VD,ED],
    numPartitions: PartitionID) extends Serializable{

  def this(ps:PartitionStrategy[VD,ED]) = this(ps, ps.graph.edges.partitions.length)

  /**
    * Denote the duplicate ratio of a partition strategy
    *
    * @return duplicate ratio
    */
  def duplication(): Double = {
    val numDistinctEdges = ps.graph.edges.distinct.count
    val numTotalEdges = ps.partitionBy(numPartitions).edges.count
    (numTotalEdges - numDistinctEdges).toDouble / numDistinctEdges.toDouble
  }

  def efficiency() = {

  }
}
