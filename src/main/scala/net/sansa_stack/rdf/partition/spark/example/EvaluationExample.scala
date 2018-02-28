package net.sansa_stack.rdf.partition.spark.example

import net.sansa_stack.rdf.partition.spark.evaluation.PartitionAlgoMetrics
import net.sansa_stack.rdf.partition.spark.algo.{ObjectHashPartition, PathPartition, SOHashPartition, SubjectHashPartition}
import net.sansa_stack.rdf.partition.spark.utils.InitialGraph
import org.apache.spark.sql.SparkSession

object EvaluationExample {

  def main(args: Array[String]): Unit = {
    val path = "src/main/resources/Clustering_sampledata1.nt"
    val master = "local[4]"
    val session = SparkSession.builder()
      .master(master)
      .appName("Evaluation")
      .getOrCreate()
    val graph = InitialGraph.apply(session, path).cache()

    val pp = new PathPartition(graph, session)
    val shp = new SubjectHashPartition(graph, session)
    val ohp = new ObjectHashPartition(graph, session)
    val sohp = new SOHashPartition(graph, session)

    val ppam = new PartitionAlgoMetrics(pp)
    val spam = new PartitionAlgoMetrics(shp)
    val opam = new PartitionAlgoMetrics(ohp)
    val sopam = new PartitionAlgoMetrics(sohp)

    println("PathPartition | SubjectHashPartition | ObjectHashPartition | SOHashPartition")
    println("Duplication: "+ppam.duplication()+" | "+spam.duplication()+" | "+opam.duplication()+" | "+sopam.duplication())
    println("Balance: "+ppam.balance()+" | "+spam.balance()+" | "+opam.balance()+" | "+sopam.balance())
    println("Efficiency: "+ppam.efficiency()+" | "+spam.efficiency()+" | "+opam.efficiency()+" | "+sopam.efficiency())
  }
}
