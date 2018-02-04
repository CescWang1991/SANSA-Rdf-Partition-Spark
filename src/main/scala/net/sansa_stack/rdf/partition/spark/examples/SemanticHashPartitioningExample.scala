package net.sansa_stack.rdf.partition.spark.examples

import net.sansa_stack.rdf.partition.spark.utils.{BaselineHashPartitions, InitialGraph, SemanticHashPartitions}
import org.apache.spark.TaskContext
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.sql.SparkSession

object SemanticHashPartitioningExample {

  def main(args: Array[String]): Unit = {
    val path = args(0)
    val master = args(1)
    val session = SparkSession.builder().master(master).appName("Semantic Hash Partitioning").getOrCreate()
    val sc = session.sparkContext
    val graph = InitialGraph.apply(session, path).cache()
    val bhp = BaselineHashPartitions.apply(graph, PartitionStrategy.RandomVertexCut)
    bhp.vertices.foreachPartition(it=>println("Partition "+TaskContext.getPartitionId+" has "+it.length+" vertices"))

    for (i<- 1 to 3) {
      val hopNum = i
      val shp = new SemanticHashPartitions(bhp, hopNum, sc)
      println(hopNum + "-hop-expansion______")
      shp.vertices.foreachPartition(it => println("Partition " + TaskContext.getPartitionId + " has " + it.length + " vertices"))
      shp.edges.foreachPartition(it => println("Partition " + TaskContext.getPartitionId + " has " + it.length + " edges"))
    }
  }
}
