package net.sansa_stack.rdf.partition.spark

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Node
import org.apache.jena.graph.Triple

class VertexPartition(val vertex:RDD[Node],val triple:RDD[Triple]) {
  private var rdfType = None:Option[Node]
  private var numPartition = None:Option[Int]
  val baseVertex: RDD[Node] = VertexPartition.setBaseVertex(vertex,triple,rdfType)

  def setRdfType(rdfType:Node): Unit ={
    this.rdfType = Some(rdfType)
  }
  def setPartitionNumber(num:Int): Unit = {
    this.numPartition = Some(num)
  }
}

object VertexPartition {
  def setBaseVertex(vertex:RDD[Node],triple:RDD[Triple], rdfType: Option[Node]): RDD[Node] = {
    vertex
  }
}
