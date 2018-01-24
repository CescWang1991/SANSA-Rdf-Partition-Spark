import net.sansa_stack.rdf.partition.spark.utils.{InitialGraph, SubjectTripleGroup}
import org.apache.spark.sql.SparkSession
object TripleGroupTest {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("Rdf Graph Partitioning").getOrCreate()
    val path = args(0)
    val graph = InitialGraph.apply(session, path).cache()
    val anchorVertices = graph.vertices.filter(vertex => vertex._1 <= 10)
    val subGraph = SubjectTripleGroup.apply(anchorVertices,graph)
    subGraph.foreach((stg)=>println(stg._1+" ; "+stg._2.edges.count()))
  }
}
