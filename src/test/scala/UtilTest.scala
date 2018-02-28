import net.sansa_stack.rdf.partition.spark.algo.PathPartition
import net.sansa_stack.rdf.partition.spark.utils._
import org.apache.spark.graphx.Edge
import org.apache.spark.sql.SparkSession

object UtilTest {
  def main(args: Array[String]): Unit = {
    val path = "src/main/resources/AnomalyDetection/clusteringOntype.nt"
    val session = SparkSession.builder().master("local[4]").appName("Rdf Graph Partitioning").getOrCreate()
    val graph = InitialGraph.apply(session, path).cache()

    /*val pathGraph = Paths.run(graph,5)
    val attr = VertexWeight.apply(graph, pathGraph, session.sparkContext)
    val svg = SVG.apply(graph,attr,4)*/
    val pps = new PathPartition(graph,session)
    //println(pps.sources.mkString("\n"))
    println(pps.partitionBy().edges.count())
  }
}
