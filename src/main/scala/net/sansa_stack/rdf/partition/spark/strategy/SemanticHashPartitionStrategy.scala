package net.sansa_stack.rdf.partition.spark.strategy

import net.sansa_stack.rdf.partition.spark.utils.TripleGroupType.TripleGroupType
import net.sansa_stack.rdf.partition.spark.utils.{TripleGroup, TripleGroupType}
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


/**
  * Semantic Hash Partition Strategy expands the partitions by assign triple group of each vertex to partitions
  * Expand Edges set E+(i) = tg(v*).edges
  *
  * @param graph target graph to be partitioned
  * @param k number of iterations
  * @param tgt type of triple groups, a set of subject(s), object(o) and subject-object(so)
  * @param sc Spark Context
  *
  * @tparam VD the vertex attribute associated with each vertex in the set.
  * @tparam ED the edge attribute associated with each edge in the set.
  *
  * @author Zhe Wang
  */
class SemanticHashPartitionStrategy[VD: ClassTag,ED: ClassTag](
    override val graph: Graph[VD,ED],
    k: Int,
    tgt: TripleGroupType,
    sc: SparkContext)
    extends PartitionStrategy(graph) with Serializable {

  private val stg = new TripleGroup(graph,tgt)
  private val neighborsBroadcast = sc.broadcast(stg.verticesGroupSet.collect())
  private val edgesBroadcast = sc.broadcast(stg.edgesGroupSet.collect())

  override def partitionBy(): Graph[VD,ED] = { partitionBy(graph.edges.partitions.length) }

  /**
    * Partition the graph with input number of partitions
    *
    * @param numPartitions
    * @return partitioned graph
    */
  override def partitionBy(numPartitions: PartitionID): Graph[VD,ED] = {
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
    for(i<-0 to k-1){
      if(i==0){
        v(i) = newVertices.mapPartitions(it => oneHopExpansionForVertices(it))
        e(i) = newVertices.mapPartitions(it => oneHopExpansionForEdges(it))
      }
      else{
        v(i) = v(i-1).mapPartitions(it => oneHopExpansionForVertices(it))
        e(i) = v(i-1).mapPartitions(it => oneHopExpansionForEdges(it))
      }
    }
    Graph[VD,ED](v(k-1),e(k-1))
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