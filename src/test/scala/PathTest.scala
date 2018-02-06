import net.sansa_stack.rdf.partition.spark.utils
import net.sansa_stack.rdf.partition.spark.utils.{DistanceOfFarthestEdge, EndToEndPaths, InitialGraph}
import org.apache.jena.graph.Node
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.SparkSession

object PathTest {

  def main(args: Array[String]): Unit = {
    val path = "./src/main/resources/Clustering_sampledata1.nt"
    val session = SparkSession.builder().master("local[4]").appName("Rdf Graph Partitioning").getOrCreate()
    val sc = session.sparkContext
    val graph = InitialGraph.apply(session, path).cache()

    EndToEndPaths.run(graph).foreach(println(_))
  }
}