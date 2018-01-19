package net.sansa_stack.rdf.partition.spark

import net.sansa_stack.rdf.spark.io.RDFReader
import org.apache.jena.graph.Node
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

abstract class OriginalGraph(val session: SparkSession,val path:String) extends RdfGraph {
  val rdfReader = new RDFReader()(session)
  override val triples = rdfReader.load(session, path).persist
  override val vertex = RdfGraph.setVertex(triples).persist
}

object OriginalGraph{

  /**
    * Decide vertices whose degree exceed a predefined threshold
    *
    * @param triple the RDD of RDF Triples
    * @param threshold a predefined value
    * @return the RDD of Nodes
    */
  def setHighDegreeVertex(triple:RDD[Triple],threshold:Int): Unit = {

  }

  /**
    * Remove Triples from an input RDD[Triple] whose subject or object is in an input RDD[Node]
    *
    * @param triple the RDD of RDF Triples
    * @param vertex the RDD of Nodes
    * @return the RDD of RDF Triples
    */
  def removeTriple(triple:RDD[Triple],vertex:RDD[Node]) : Unit = {

  }
}
