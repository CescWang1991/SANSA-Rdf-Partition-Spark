package net.sansa_stack.rdf.partition.spark.strategy

import net.sansa_stack.rdf.partition.spark.utils.TripleGroupType.TripleGroupType
import net.sansa_stack.rdf.partition.spark.utils.{TripleGroup, TripleGroupType}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag


/**
  * Semantic Hash Partition Strategy expands the partitions by assign triple group of each vertex to partitions
  * Expand Edges set E+(i) = tg(v*).edges
  *
  * @param graph target graph to be partitioned
  * @param session spark session
  * @param numPartitions number of partitions
  * @param numHop number of iterations
  * @param tgt type of triple groups, a set of subject(s), object(o) and subject-object(so)
  *
  * @tparam VD the vertex attribute associated with each vertex in the set.
  * @tparam ED the edge attribute associated with each edge in the set.
  *
  * @author Zhe Wang
  */
class SemanticHashPartitionStrategy[VD: ClassTag,ED: ClassTag](
    override val graph: Graph[VD,ED],
    override val session: SparkSession,
    numPartitions: PartitionID,
    private var numHop: Int,
    private var tgt: TripleGroupType)
  extends PartitionStrategy(graph, session, numPartitions) with Serializable {

  /**
    * Constructs a default instance with default parameters {numPartitions: graph partitions,
    * numHop: 2, TripleGroupType: subject}
    *
    */
  def this(graph: Graph[VD,ED], session: SparkSession) = {
    this(graph, session, graph.edges.partitions.length, 2, TripleGroupType.s)
  }

  /**
    * Constructs a default instance with default parameters {numHop: 2, TripleGroupType: subject}
    *
    */
  def this(graph: Graph[VD,ED], session: SparkSession, numPartitions: Int) = {
    this(graph, session, numPartitions, 2, TripleGroupType.s)
  }

  /**
    * Sets the number of iterations (default: 2)
    *
    */
  def setNumHop(numHop: Int): this.type = {
    require(numHop > 0,
      s"Number of iterations must be positive but got ${numHop}")
    this.numHop = numHop
    this
  }

  /**
    * Sets the type of triple group type (default: subject)
    *
    */
  def setTripleGroupType(tgt: TripleGroupType): this.type = {
    require(TripleGroupType.values.contains(tgt),
      s"Triple Group Type must be {s, o, so} but got ${tgt}")
    this.tgt = tgt
    this
  }

  private val sc = session.sparkContext

  override def partitionBy(): Graph[VD,ED] = {
    val stg = new TripleGroup(graph,tgt)
    val edgesBroadcast = sc.broadcast(stg.edgesGroupSet.collectAsMap)
    val e = new Array[RDD[Edge[ED]]](numHop)
    tgt match {
      case TripleGroupType.s | TripleGroupType.so =>
        val bhp = graph.partitionBy(PartitionStrategy.EdgePartition1D,numPartitions).cache()
        for(i<-0 to numHop-1){
          if(i==0){
            e(i) = bhp.edges
          }
          else{
            e(i) = e(i-1).mapPartitions{ iter =>
              val initialEdges = iter.toArray
              val expandEdges = initialEdges.map(e => e.dstId).distinct.flatMap(vid =>
                  edgesBroadcast.value.get(vid)).flatten
              initialEdges.++(expandEdges).distinct.toIterator
            }
          }
        }
        Graph[VD,ED](bhp.vertices,e(numHop-1))
      case TripleGroupType.o =>
        val bhp = graph.reverse.partitionBy(PartitionStrategy.EdgePartition1D,numPartitions).reverse.cache()
        for(i<-0 to numHop-1){
          if(i==0){
            e(i) = bhp.edges
          }
          else{
            e(i) = e(i-1).mapPartitions{ iter =>
              val initialEdges = iter.toArray
              val expandEdges = initialEdges.map(e => e.srcId).distinct.flatMap(vid =>
                edgesBroadcast.value.get(vid)).flatten
              initialEdges.++(expandEdges).distinct.toIterator
            }
          }
        }
        Graph[VD,ED](bhp.vertices,e(numHop-1))
    }
  }

  override def getVertices(): RDD[(VertexId, VD)] = {
    val stg = new TripleGroup(graph,tgt)
    val verticesBroadcast = sc.broadcast(stg.verticesGroupSet.collectAsMap)
    val v = new Array[RDD[(VertexId, VD)]](numHop)
    tgt match {
      case TripleGroupType.s | TripleGroupType.so =>
        val bhp = graph.partitionBy(PartitionStrategy.EdgePartition1D,numPartitions).cache()
        for(i<-0 to numHop-1){
          if(i==0){
            v(i) = bhp.triplets.mapPartitions{ iter =>
              val initialVertices = iter.map(e=>(e.srcId,e.srcAttr)).toArray.distinct
              val expandVertices = initialVertices.flatMap{ case(vid,_) =>
                verticesBroadcast.value.get(vid)}.flatten
              initialVertices.++(expandVertices).distinct.toIterator
            }
          }
          else{
            v(i) = v(i-1).mapPartitions{ iter =>
              val initialVertices = iter.toArray
              val expandVertices = initialVertices.flatMap{ case(vid,_) =>
                verticesBroadcast.value.get(vid)}.flatten
              initialVertices.++(expandVertices).distinct.toIterator
            }
          }
        }
        v(numHop-1)
      case TripleGroupType.o =>
        val bhp = graph.reverse.partitionBy(PartitionStrategy.EdgePartition1D,numPartitions).reverse.cache()
        for(i<-0 to numHop-1){
          if(i==0){
            v(i) = bhp.triplets.mapPartitions{ iter =>
              val initialVertices = iter.map(e=>(e.dstId,e.dstAttr)).toArray.distinct
              val expandVertices = initialVertices.flatMap{ case(vid,_) =>
                verticesBroadcast.value.get(vid)}.flatten
              initialVertices.++(expandVertices).distinct.toIterator
            }
          }
          else{
            v(i) = v(i-1).mapPartitions{ iter =>
              val initialVertices = iter.toArray
              val expandVertices = initialVertices.flatMap{ case(vid,_) =>
                verticesBroadcast.value.get(vid)}.flatten
              initialVertices.++(expandVertices).distinct.toIterator
            }
          }
        }
        v(numHop-1)
    }
  }
}