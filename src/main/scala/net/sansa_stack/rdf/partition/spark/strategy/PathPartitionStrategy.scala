package net.sansa_stack.rdf.partition.spark.strategy

import net.sansa_stack.rdf.partition.spark.utils.{EndToEndPaths, StartVerticesGroup}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag


/**
  * Path Partition Strategy expand the partitions by assign path groups of all start vertices
  * Expand Edges set E+(i) = pg(sv*).edges
  *
  * @param graph target graph to be partitioned
  * @param session spark session
  * @param numPartitions number of partitions
  *
  * @tparam VD the vertex attribute associated with each vertex in the set.
  * @tparam ED the edge attribute associated with each edge in the set.
  *
  * @author Zhe Wang
  */
class PathPartitionStrategy[VD: ClassTag,ED: ClassTag](
    override val graph: Graph[VD,ED],
    override val session: SparkSession,
    numPartitions: PartitionID)
  extends PartitionStrategy(graph,session,numPartitions) with Serializable {

  graph.cache()
  private val pathGraph = EndToEndPaths.run(graph)
  private val pathLists = pathGraph.map{ case(_,list) => list }.reduce((list1, list2) => list1.++(list2))
  private val sources = EndToEndPaths.setSrcVertices(graph)

  override def partitionBy() = {
    val pathInEdge = pathGraph.collect.map{ case(vid,pathList) =>
      val newPath = pathList.flatMap(path =>
        path.sliding(2).map(pair =>
          graph.edges.filter(e =>
            e.srcId==pair(0)&&e.dstId==pair(1)).first).toList).distinct
      (vid,newPath)
    }
    val svg = StartVerticesGroup.run(graph,pathLists,numPartitions)
    val newEdges = graph.edges.mapPartitionsWithIndex{ case(pid,_) =>
      pathInEdge.filter{ case(vid,_) =>
        sources.filter(getPartition(_, svg, numPartitions) == pid).contains(vid)
      }.flatMap{ case(_,it) => it }.distinct.toIterator
    }.cache()

    Graph[VD,ED](graph.vertices,newEdges)
  }

  private def getPartition(src:VertexId,svg:Array[Array[VertexId]],numPartitions:PartitionID) : PartitionID = {
    svg.flatMap(array =>
      array.flatMap(vid =>
        Map(vid -> svg.indexOf(array))
      )
    ).toMap.getOrElse(src,numPartitions)
  }

  override def getVertices() = {
    val pathInVertex = pathGraph.collect.map{ case(vid,pathList) =>
      val newPath = pathList.flatMap(path =>
        path.map(id =>
          graph.vertices.filter(v =>
            v._1==id).first)).distinct
      (vid,newPath)
    }
    val svg = StartVerticesGroup.run(graph,pathLists,numPartitions)
    val newVertices = graph.vertices.mapPartitionsWithIndex{ case(pid,_) =>
      pathInVertex.filter{ case(vid,_) =>
        sources.filter(getPartition(_, svg, numPartitions) == pid).contains(vid)
      }.flatMap{ case(_,it) => it }.distinct.toIterator
    }.cache()

    newVertices
  }
}