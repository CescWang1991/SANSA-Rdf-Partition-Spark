package net.sansa_stack.rdf.partition.spark.example

import net.sansa_stack.rdf.partition.spark.algo.SOHashPartition
import net.sansa_stack.rdf.partition.spark.utils.InitialGraph
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

object SemanticHashPartitionExample {

  def main(args: Array[String]): Unit = {
    val path = "src/main/resources/SilviaClustering_HairStylist_TaxiDriver.nt"
    val master = "local[4]"
    val session = SparkSession.builder().master(master).appName("Semantic Hash Partitioning").getOrCreate()
    val graph = InitialGraph.apply(session, path).cache()
    //graph.partitionBy(PartitionStrategy.EdgePartition1D,4).edges.foreachPartition(it => println("Partition " + TaskContext.getPartitionId + " has " + it.toList.distinct.length + " edges"))

    /*val shps = new SubjectHashPartitionStrategy(graph, session)
    shps.partitionBy().edges.foreachPartition(it =>
      println("Partition " + TaskContext.getPartitionId + " has " + it.toList.distinct.length + " edges"))*/

    /*val ohps = new ObjectHashPartitionStrategy(graph, session).setNumIterations(3)
    ohps.partitionBy().edges.foreachPartition(it =>
      println("Partition " + TaskContext.getPartitionId + " has " + it.toList.distinct.length + " edges"))*/

    val sohps = new SOHashPartition(graph,session).setNumIterations(2)
    sohps.partitionBy().edges.foreachPartition(it =>
      println("Partition " + TaskContext.getPartitionId + " has " + it.toList.distinct.length + " edges"))
  }
}
