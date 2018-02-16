package net.sansa_stack.rdf.partition.spark.utils

import org.apache.spark.graphx.{Graph, PartitionID, VertexId}

import scala.reflect.ClassTag


/**
  * An object which to merge a vertex by assigning all end to end paths into a path group that pass through this vertex
  * It divides all start vertices into k nonempty and disjoint sets (k = number of partitions)
  *
  * @author Zhe Wang
  */
object StartVerticesGroup extends Serializable {

  /**
    * Method to generate start vertices groups.
    *
    * @param graph the graph for which to make path partitioning.
    * @param pathLists A list contains all paths as list of VertexId
    * @param numPartitions number of partitions.
    * @return Array of start vertices sets, each set means put all paths from those start vertices into one partition
    */
  def run[VD: ClassTag,ED:ClassTag](
      graph: Graph[VD,ED],
      pathLists: EndToEndPaths.pathList,
      numPartitions: PartitionID): Array[Array[VertexId]] = {

    val vertexAttribute = VertexAttribute.apply(graph,pathLists)
    vertexAttribute.cache()
    val src = EndToEndPaths.setSrcVertices(graph)

    var mergeVertices = vertexAttribute.vertices.filter{ case(vid,_) => (!src.contains(vid)) }
      .sortBy{ case(_,attr) => (attr._1.toInt,attr._2.length) }
    var startVerticesGroup = src.map(vid=>Array(vid))
    while(startVerticesGroup.lengthCompare(numPartitions)>0 && mergeVertices.count()!=0) {
      val mergeVertex = mergeVertices.first()._1
      startVerticesGroup = merge(mergeVertex, startVerticesGroup, vertexAttribute, src, numPartitions)
      mergeVertices = mergeVertices.filter{ case(vid,_) => (vid != mergeVertex) }
    }

    startVerticesGroup = startVerticesGroup.sortBy(array=>array.length)(Ordering[Int].reverse)
    while(startVerticesGroup.lengthCompare(numPartitions)>0){
      val lastTwo = startVerticesGroup.slice(startVerticesGroup.length-2,startVerticesGroup.length)
      startVerticesGroup = startVerticesGroup.dropRight(2) :+ lastTwo(0).++(lastTwo(1))
    }
    startVerticesGroup
  }

  private def merge[ED:ClassTag](
      vid:VertexId,
      svg:Array[Array[VertexId]],
      vertexAttribute:Graph[(Int,List[VertexId]),ED],
      src:Array[VertexId],
      numPartitions: PartitionID) : Array[Array[VertexId]]={
    val startVerticesToMerge = vertexAttribute.vertices.filter(_._1==vid).map(_._2._2).first()
    val groupsToMerge = svg.filter{group=>
      startVerticesToMerge.exists(sv=>group.contains(sv))
    }
    if(groupsToMerge.lengthCompare(1)==0) {
      svg
    }
    else {
      val svgToMerge = groupsToMerge.reduce((group1,group2)=>group1.++(group2))
      if(svgToMerge.lengthCompare(Math.ceil(src.length.toDouble / numPartitions.toDouble).toInt)<=0){
        svg.diff(groupsToMerge).:+(svgToMerge)
      }
      else{ svg }
    }
  }
}
