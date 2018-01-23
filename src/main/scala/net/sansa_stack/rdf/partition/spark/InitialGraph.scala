package net.sansa_stack.rdf.partition.spark

import net.sansa_stack.rdf.spark.graph.LoadGraph
import net.sansa_stack.rdf.spark.io.{NTripleReader, RDFReader}
import org.apache.spark.sql.SparkSession

class InitialGraph(val session: SparkSession, val path:String) extends Serializable {
  val reader = NTripleReader.load(session,path) //Currently only load N-Triple files for testing
  val graph = LoadGraph.apply(reader).persist()
}

object InitialGraph{

  /**
    *
    *
    * param triple the RDD of RDF Triples
    * param threshold a predefined value
    * return the RDD of Nodes
    */

  /**
    *
    *
    * param triple the RDD of RDF Triples
    * param vertex the RDD of Nodes
    * return the RDD of RDF Triples
    */

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("Rdf Graph Partitioning").getOrCreate()
    val path = args(0)
    val ig = new InitialGraph(session,path)
    ig.graph.edges.collect.foreach(println(_))
    println(ig.graph.triplets.count)
  }
}
