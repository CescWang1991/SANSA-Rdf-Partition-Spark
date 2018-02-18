package net.sansa_stack.rdf.partition.spark.partitionStrategy

import net.sansa_stack.rdf.partition.spark.utils.{EndToEndPaths, StartVerticesGroup}
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Assigns start vertices to partitions using vertex ID
  */
class PathPartitionStrategy[VD: ClassTag,ED:ClassTag](graph: Graph[VD,ED]) extends Serializable {

  graph.cache()
  val pathGraph = EndToEndPaths.run(graph)
  val pathLists = pathGraph.map{ case(_,list) => list }.reduce((list1, list2) => list1.++(list2))
  val sources = EndToEndPaths.setSrcVertices(graph)
  val destinations = EndToEndPaths.setDstVertices(graph)

  def partitionBy(): Graph[VD,ED] = { partitionBy(graph.edges.partitions.length) }

  def partitionBy(numPartitions: PartitionID): Graph[VD,ED] = {
    val pathInEdge = pathGraph.collect.map{ case(vid,pathList) =>
      val newPath = pathList.flatMap(path =>
        path.sliding(2).map(pair =>
          graph.edges.filter(e =>
            e.srcId==pair(0)&&e.dstId==pair(1)).first).toList).distinct
      (vid,newPath)
    }
    val pathInVertex = pathGraph.collect.map{ case(vid,pathList) =>
      val newPath = pathList.flatMap(path =>
        path.map(id =>
          graph.vertices.filter(v =>
            v._1==id).first)).distinct
      (vid,newPath)
    }

    val svg = StartVerticesGroup.run(graph,pathLists,numPartitions)

    val newEdges = graph.edges.mapPartitionsWithIndex{ case(pid,_) =>
      pathInEdge.filter{ case(vid,_) =>
        sources.filter(getPartition(_, svg, numPartitions) == pid).contains(vid)
      }.flatMap{ case(_,it) => it }.distinct.toIterator
    }.cache()

    val newVertices = graph.vertices.mapPartitionsWithIndex{ case(pid,_) =>
      pathInVertex.filter{ case(vid,_) =>
        sources.filter(getPartition(_, svg, numPartitions) == pid).contains(vid)
      }.flatMap{ case(_,it) => it }.distinct.toIterator
    }.cache()

    Graph[VD,ED](newVertices,newEdges)
  }

  private def getPartition(src:VertexId,svg:Array[Array[VertexId]],numPartitions:PartitionID) : PartitionID = {
    svg.flatMap(array =>
      array.flatMap(vid =>
        Map(vid -> svg.indexOf(array))
      )
    ).toMap.getOrElse(src,numPartitions)
  }
}
