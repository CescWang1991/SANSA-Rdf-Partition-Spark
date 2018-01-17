package net.sansa_stack.rdf.partition.spark

import net.sansa_stack.rdf.spark.io.RDFReader
import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class OriginalGraph(var session: SparkSession,var path:String) extends RdfGraph {
  val rdfReader = new RDFReader()
  override val triples = rdfReader.load(session, path)
  override val vertex = RdfGraph.setVertex(triples)

  def vertexDegree(threshold: Int): Boolean = {
    false
  }

  def removeRdfType(vertexDegree: Boolean) :Unit = {

  }
}
