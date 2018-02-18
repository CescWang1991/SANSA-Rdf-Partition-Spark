package net.sansa_stack.rdf.partition.spark.example

import net.sansa_stack.rdf.partition.spark.strategy.PathPartitionStrategy
import net.sansa_stack.rdf.partition.spark.utils.InitialGraph
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession


object PathPartitionExample {

  def main(args: Array[String]): Unit = {
    val path = "./src/main/resources/Clustering_sampledata1.nt"
    val session = SparkSession.builder().master("local[4]").appName("Rdf Graph Partitioning").getOrCreate()
    val graph = InitialGraph.apply(session, path).cache()

    val pps = new PathPartitionStrategy(graph)
    //pps.partitionBy(2).vertices.foreach(v=>println(TaskContext.getPartitionId()+","+v))
    pps.partitionBy(2).triplets.foreach(t=>println(TaskContext.getPartitionId()+","+t))
  }
}