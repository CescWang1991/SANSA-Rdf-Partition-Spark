package net.sansa_stack.rdf.partition.spark.utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag


/**
  * Semantic Hash Partitions are expanded from Baseline Hash Partitions
  * For Baseline Hash Partition P(i), every v∈V(i) has triple groups
  * Expand Vertices set V+(i) = V(i)∪tg(v*).vertices
  * Expand Edges set E+(i) = tg(v*).edges
  *
  * @author Zhe Wang
  */
class SemanticHashPartitions[VD: ClassTag,ED: ClassTag](
   bhp: Graph[VD,ED],
   k: Int,
   sc: SparkContext) extends Serializable {

  bhp.cache()
  private val stg = new TripleGroup(bhp,TripleGroupType.s)
  private val neighborsBroadcast = sc.broadcast(stg.verticesGroupSet.collect())
  private val edgesBroadcast = sc.broadcast(stg.edgesGroupSet.collect())
  private val hopNum = k

  val vertices = kHopExpansion(hopNum)._1
  val edges = kHopExpansion(hopNum)._2
  val graph = Graph[VD,ED](vertices,edges)

  private def kHopExpansion(k:Int): (RDD[(VertexId,VD)],RDD[Edge[ED]]) = {
    val temp = bhp.vertices
    val v = new Array[RDD[(VertexId,VD)]](k)
    val e = new Array[RDD[Edge[ED]]](k)
    for(i<-0 to k-1){
      if(i==0){
        v(i) = temp.mapPartitions(it => oneHopExpansionForVertices(it,neighborsBroadcast.value))
        e(i) = temp.mapPartitions(it => oneHopExpansionForEdges(it,edgesBroadcast.value))
      }
      else{
        v(i) = v(i-1).mapPartitions(it =>oneHopExpansionForVertices(it,neighborsBroadcast.value))
        e(i) = v(i-1).mapPartitions(it => oneHopExpansionForEdges(it,edgesBroadcast.value))
      }
    }
    (v(k-1),e(k-1))
  }

  private def oneHopExpansionForVertices(
    iterator:Iterator[(VertexId,VD)],
    verticesSet: Array[(VertexId,Array[(VertexId,VD)])]):Iterator[(VertexId,VD)] = {
    val anchorVertices = iterator.toArray
    val expandVertices = anchorVertices.flatMap(vertex =>
      verticesSet.find{ case(anchorVertexId,_) =>
        anchorVertexId == vertex._1}.get._2
    )
    expandVertices.++(anchorVertices).distinct.toIterator
  }

  private def oneHopExpansionForEdges(iterator:Iterator[(VertexId,VD)], edgesSet: Array[(VertexId,Array[Edge[ED]])]): Iterator[Edge[ED]] = {
    val verticesWithEdgeGroupSet = edgesSet.map{ case(id,_) => id}
    val anchorVertices = iterator.filter{ case(id,_)=>verticesWithEdgeGroupSet.contains(id) }.toArray
    val expandEdges = anchorVertices.flatMap(vertex =>
      edgesSet.find{ case(anchorVertexId,_) =>
        anchorVertexId == vertex._1}.get._2
    )
    expandEdges.toIterator
  }
}

