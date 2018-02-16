import net.sansa_stack.rdf.partition.spark.utils.InitialGraph
import org.apache.jena.graph.Node
import org.apache.spark.graphx.{Edge, Graph, Pregel, VertexId}
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

object EndToEndEdgeTest {
  def main(args: Array[String]): Unit = {
    val path = "./src/main/resources/Clustering_sampledata1.nt"
    val session = SparkSession.builder().master("local[4]").appName("Rdf Graph Partitioning").getOrCreate()
    val graph = InitialGraph.apply(session, path).cache()

    type pathList = List[List[Edge[Node]]]
    //val edges = graph.edges.take(3)
    val source = setSrcVertices(graph)
    val destination = setDstVertices(graph)

    val pathGraph = graph.vertices.mapValues(_ => makeList())
    val initialMessage = makeList()

  }

  private def makeList(x: Edge[Node]*) = List(List(x: _*))

  def setSrcVertices[VD: ClassTag,ED: ClassTag](graph:Graph[VD,ED]): Array[VertexId] = {
    val ops = graph.ops
    val src = graph.vertices.map(v=>v._1).subtract(ops.inDegrees.map(v=>v._1)).collect()
    src
  }

  def setDstVertices[VD: ClassTag,ED: ClassTag](graph:Graph[VD,ED]): Array[VertexId] = {
    val ops = graph.ops
    val dst = graph.vertices.map(v=>v._1).subtract(ops.outDegrees.map(v=>v._1)).collect()
    dst
  }
}
