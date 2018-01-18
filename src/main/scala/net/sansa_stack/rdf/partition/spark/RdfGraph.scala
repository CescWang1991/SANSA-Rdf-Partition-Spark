package net.sansa_stack.rdf.partition.spark

import org.apache.jena.graph.Node
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD

abstract class RdfGraph {
  val vertex: RDD[Node]
  val triples: RDD[Triple]
}

object RdfGraph{
  def setVertex(triples: RDD[Triple]): RDD[Node] = {
    val subjects = triples.map(triple => triple.getMatchSubject)
    val objects = triples.map(triple => triple.getMatchObject)
    subjects.union(objects).distinct
  }
}

