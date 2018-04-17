package net.sansa_stack.examples.spark.query

import net.sansa_stack.rdf.query.graph.jena.BasicGraphPattern
import net.sansa_stack.rdf.query.graph.matching.GenerateSolutionMappings
import net.sansa_stack.rdf.spark.graph.LoadGraph
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.sql.SparkSession

object GraphQuery {
  def main(args: Array[String]): Unit = {
    val ntPath = "src/resources/Clustering_sampledata.nt"
    val sqPath = "src/resources/QueryBasic.txt"
    val session = SparkSession.builder()
      .master("local[*]")
      .appName("SANSA - Graph Query")
      .getOrCreate()
    val rdfGraph = LoadGraph.apply (NTripleReader.load (session, ntPath))
    //val bgp = new BasicGraphPattern(session.sparkContext, sqPath)
    rdfGraph.triplets.foreach(println(_))
    //bgp.graph.triplets.foreach(println(_))
    //println(GenerateSolutionMappings.run(rdfGraph, bgp.triplePattern, session).length)
  }
}
