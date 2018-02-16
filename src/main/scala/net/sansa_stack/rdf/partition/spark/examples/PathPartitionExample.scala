package net.sansa_stack.rdf.partition.spark.examples

import net.sansa_stack.rdf.partition.spark.partitionStrategy.PathPartitionStrategy
import net.sansa_stack.rdf.partition.spark.utils.InitialGraph
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession


object PathPartitionExample {

  def main(args: Array[String]): Unit = {
    val path = "./src/main/resources/Clustering_sampledata1.nt"
    val session = SparkSession.builder().master("local[4]").appName("Rdf Graph Partitioning").getOrCreate()
    val graph = InitialGraph.apply(session, path).cache()

    val pps = new PathPartitionStrategy(graph)
    pps.partitionBy(2).triplets.foreach(v=>println(TaskContext.getPartitionId()+","+v))
  }
}