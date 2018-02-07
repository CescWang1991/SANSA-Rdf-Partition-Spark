package net.sansa_stack.rdf.partition.spark.utils

import org.apache.spark.graphx.{Graph, VertexId}
import scala.reflect.ClassTag

/**
  * Vertex Attribute denotes the weight and start vertices list for each vertex v
  * Vertex Weight is the number of paths that contain v.
  * Start vertices list is a list of start vertices S(v) for each vertex v,
  * where S(v) contains all the start vertices that can reach v.
  *
  * @author Zhe Wang
  */
object VertexAttribute {

  type attribute = (Int,List[VertexId])

  /**
    * Generate the vertex attribute for each vertex
    *
    * @tparam ED the edge attribute type (not used in the computation)
    * @param graph the graph for which to generate attributes
    * @return a graph where each vertex attribute is a tuple containing weight and start vertices list
    */
  def apply[VD,ED: ClassTag](graph: Graph[VD,ED], allPaths: EndToEndPaths.pathList): Graph[attribute,ED] = {

    graph.mapVertices{ case(vid,_) =>  GenerateVertexAttribute(vid,allPaths)}
  }

  private def GenerateVertexAttribute(vid:VertexId,allPaths: EndToEndPaths.pathList): attribute = {
    val vertexPaths = allPaths.filter(list=>list.contains(vid))
    val vertexWeight = vertexPaths.length
    val startVerticesList = vertexPaths.map(path=>path.head).distinct
    (vertexWeight,startVerticesList)
  }
}