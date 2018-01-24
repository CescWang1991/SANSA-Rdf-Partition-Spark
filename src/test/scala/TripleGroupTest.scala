import net.sansa_stack.rdf.partition.spark.utils._
import org.apache.spark.sql.SparkSession

object TripleGroupTest {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("Rdf Graph Partitioning").getOrCreate()
    val path = args(0)
    val graph = InitialGraph.apply(session, path).cache()
    val vertices = graph.vertices.filter(vertex => vertex._1 <= 70 && vertex._1 >= 60)
    val subGraph = SOTripleGroup.apply(vertices,graph)
    subGraph.foreach((tg)=>println(tg._1+" ; "+tg._2.edges.count()))
    println("--------------")
    val subGraph_1 = SubjectTripleGroup.apply(vertices,graph)
    subGraph_1.foreach((tg)=>println(tg._1+" ; "+tg._2.edges.count()))
    println("--------------")
    val subGraph_2 = ObjectTripleGroup.apply(vertices,graph)
    subGraph_2.foreach((tg)=>println(tg._1+" ; "+tg._2.edges.count()))
    println("--------------")
  }
}
