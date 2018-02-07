import net.sansa_stack.rdf.partition.spark.utils.{EndToEndPaths, InitialGraph, VertexAttribute}
import org.apache.spark.sql.SparkSession

object PathTest {

  def main(args: Array[String]): Unit = {
    val path = "./src/main/resources/Clustering_sampledata1.nt"
    val session = SparkSession.builder().master("local[4]").appName("Rdf Graph Partitioning").getOrCreate()
    val sc = session.sparkContext
    val graph = InitialGraph.apply(session, path).cache()

    val allPaths = EndToEndPaths.run(graph)
    VertexAttribute.apply(graph,allPaths).vertices.foreach(println(_))
  }
}