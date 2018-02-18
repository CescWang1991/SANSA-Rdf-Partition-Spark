package net.sansa_stack.rdf.partition.spark.example

import net.sansa_stack.rdf.partition.spark.strategy.SemanticHashPartitionStrategy
import net.sansa_stack.rdf.partition.spark.utils.{InitialGraph, TripleGroupType}
import org.apache.spark.TaskContext
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.sql.SparkSession

object SemanticHashPartitionExample {

  def main(args: Array[String]): Unit = {
    val path = "src/main/resources/SilviaClustering_HairStylist_TaxiDriver.nt"
    val master = "local[4]"
    val session = SparkSession.builder().master(master).appName("Semantic Hash Partitioning").getOrCreate()
    val sc = session.sparkContext
    val graph = InitialGraph.apply(session, path).cache()
    //graph.partitionBy(PartitionStrategy.EdgePartition1D,4).edges.foreachPartition(it => println("Partition " + TaskContext.getPartitionId + " has " + it.toList.distinct.length + " edges"))

    for (i<- 1 to 3) {
      val hopNum = i
      val shps = new SemanticHashPartitionStrategy(graph, hopNum, TripleGroupType.s, sc)
      println(hopNum + "-hop-expansion______")
      shps.partitionBy().edges.foreachPartition(it => println("Partition " + TaskContext.getPartitionId + " has " + it.length + " edges"))
    }
  }
}
