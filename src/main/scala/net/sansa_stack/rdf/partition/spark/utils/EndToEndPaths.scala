package net.sansa_stack.rdf.partition.spark.utils

import org.apache.spark.graphx._
import scala.reflect.ClassTag

/**
  * A path List(v0,v1,...,vm) is called an end-to-end path that
  * v0 is a source vertex that has no incoming edges
  * vm is a sink vertex that has no outcoming edges
  * List all end-to-end-paths in a graph
  *
  * Remark: the situation of circles in graphs currently is not considered right now
  *
  * @author Zhe Wang
  */
object EndToEndPaths {

  type pathList = List[List[VertexId]]
  private def makeList(x: VertexId*) = List(List(x: _*))

  /**
    * List all end-to-end-paths
    *
    * @tparam ED the edge attribute type (not used in the computation)
    * @param graph the graph for which to list all end-to-end-paths
    * @return a list that contains all end-to-end-paths, one end-to-end-path is a list of vertices in sequence
    */
  def run[VD, ED: ClassTag](graph:Graph[VD,ED]) : pathList = {
    val ops = graph.ops
    val source = graph.vertices.map(v=>v._1).subtract(ops.inDegrees.map(v=>v._1)).collect()
    val destination = graph.vertices.map(v=>v._1).subtract(ops.outDegrees.map(v=>v._1)).collect()

    val pathGraph = graph.mapVertices { (vid, attr) =>
      if(destination.contains(vid)){
        makeList(vid)
      }
      else{
        makeList()
      }
    }

    val initialMessage = makeList()

    def vertexProgram(id: VertexId, attr: pathList, msg: pathList): pathList = {
      if(msg.head.isEmpty){
        attr
      }
      else{
        if(source.contains(id)){
          if(attr.head.isEmpty){ msg.map(list => list.+:(id)) }
          else{ msg.map(list => list.+:(id)).++(attr) }
        }
        else{
          msg.map(list => list.+:(id))
        }
      }
    }

    def sendMessage(edge: EdgeTriplet[pathList,_]): Iterator[(VertexId,pathList)] = {
      val attr = edge.dstAttr
      if(attr.head.isEmpty){
        Iterator.empty
      }
      else{
        Iterator((edge.srcId, attr))
      }
    }

    def mergeMessage(msg1: pathList, msg2: pathList): pathList = {
      msg1.++(msg2)
    }

    val pathsFromSource = Pregel.apply(pathGraph,initialMessage, maxIterations=7, activeDirection = EdgeDirection.In)(vertexProgram, sendMessage, mergeMessage)
      .vertices.filter{ case(vid,list) => source.contains(vid)}.map{ case(vid,list) => list }

    pathsFromSource.reduce((list1, list2) => list1.++(list2))
  }
}
