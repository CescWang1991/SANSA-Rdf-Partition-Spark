package net.sansa_stack.rdf.partition.spark.partitionStrategy

import net.sansa_stack.rdf.partition.spark.utils.TripleGroupType.TripleGroupType
import net.sansa_stack.rdf.partition.spark.utils.{TripleGroup, TripleGroupType}
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.graphx._
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
class SemanticHashPartitionStrategy[VD: ClassTag,ED: ClassTag](
   graph: Graph[VD,ED],
   k: Int,
   tgt: TripleGroupType,
   sc: SparkContext) extends Serializable {

  private val stg = new TripleGroup(graph,tgt)
  private val neighborsBroadcast = sc.broadcast(stg.verticesGroupSet.collect())
  private val edgesBroadcast = sc.broadcast(stg.edgesGroupSet.collect())

  def partitionBy(): Graph[VD,ED] = { partitionBy(graph.edges.partitions.length) }

  def partitionBy(numPartitions: PartitionID): Graph[VD,ED] = {
    val bhp = graph.partitionBy(PartitionStrategy.EdgePartition1D,numPartitions).cache()
    val verticesList = {
      tgt match{
        case TripleGroupType.s =>
          bhp.triplets.map(et=>(TaskContext.getPartitionId,(et.srcId,et.srcAttr))).distinct.collect
        case TripleGroupType.o =>
          bhp.triplets.map(et=>(TaskContext.getPartitionId,(et.dstId,et.dstAttr))).distinct.collect
        case TripleGroupType.so =>
          bhp.triplets.map(et=>(TaskContext.getPartitionId,(et.srcId,et.srcAttr)))
            .++(bhp.triplets.map(et=>(TaskContext.getPartitionId,(et.dstId,et.dstAttr))))
            .distinct.collect
      }
    }
    val newVertices = graph.vertices.mapPartitionsWithIndex{ case(pid,_)=>
      verticesList.filter{ case(p,_) =>
        p == pid
      }.map(pv=>pv._2).toIterator
    }

    val v = new Array[RDD[(VertexId,VD)]](k+1)
    val e = new Array[RDD[Edge[ED]]](k+1)
    for(i<-0 to k){
      if(i==0){
        v(i) = newVertices.mapPartitions(it => oneHopExpansionForVertices(it))
        e(i) = newVertices.mapPartitions(it => oneHopExpansionForEdges(it))
      }
      else{
        v(i) = v(i-1).mapPartitions(it => oneHopExpansionForVertices(it))
        e(i) = v(i-1).mapPartitions(it => oneHopExpansionForEdges(it))
      }
    }
    Graph[VD,ED](v(k),e(k))
  }

  private def oneHopExpansionForVertices(iterator:Iterator[(VertexId,VD)]):Iterator[(VertexId,VD)] = {
    val verticesSet = neighborsBroadcast.value
    val anchorVertices = iterator.toArray
    val expandVertices = anchorVertices.flatMap(vertex =>
      verticesSet.find{ case(anchorVertexId,_) =>
        anchorVertexId == vertex._1}.get._2
    )
    expandVertices.++(anchorVertices).distinct.toIterator
  }

  private def oneHopExpansionForEdges(iterator:Iterator[(VertexId,VD)]): Iterator[Edge[ED]] = {
    val edgesSet = edgesBroadcast.value
    val verticesWithEdgeGroupSet = edgesSet.map{ case(id,_) => id}
    val anchorVertices = iterator.filter{ case(id,_)=>verticesWithEdgeGroupSet.contains(id) }.toArray
    val expandEdges = anchorVertices.flatMap(vertex =>
      edgesSet.find{ case(anchorVertexId,_) =>
        anchorVertexId == vertex._1}.get._2
    )
    expandEdges.distinct.toIterator
  }
}