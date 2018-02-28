package net.sansa_stack.rdf.partition.spark.example

import net.sansa_stack.rdf.partition.spark.algo.PathPartition
import net.sansa_stack.rdf.partition.spark.utils.InitialGraph
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession


object PathPartitionExample {

  def main(args: Array[String]): Unit = {
    //val path = "./src/main/resources/rdf.nt"
    val path = "src/main/resources/Clustering_sampledata1.nt"
    val session = SparkSession.builder().master("local[4]").appName("Rdf Graph Partitioning").getOrCreate()
    val graph = InitialGraph.apply(session, path).cache()

    val pps = new PathPartition(graph, session, 2)
    graph.triplets.foreachPartition(it=>println(TaskContext.getPartitionId()+","+it.length))
    pps.partitionBy().triplets.foreachPartition(it=>println(TaskContext.getPartitionId()+","+it.length))
  }
}