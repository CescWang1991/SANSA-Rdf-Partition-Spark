package net.sansa_stack.rdf.partition.spark.strategy

import org.apache.spark.graphx.{Graph, PartitionID}

import scala.reflect.ClassTag

abstract class PartitionStrategy[VD: ClassTag,ED: ClassTag](val graph: Graph[VD,ED]) extends Serializable {

  def partitionBy(): Graph[VD,ED]

  def partitionBy(numPartitions: PartitionID): Graph[VD,ED]
}