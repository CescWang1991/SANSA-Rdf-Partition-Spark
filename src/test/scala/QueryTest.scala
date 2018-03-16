import kafka.network.RequestChannel.Session
import net.sansa_stack.rdf.partition.spark.query.{MatchCandidate, MatchSet, TriplePattern}
import net.sansa_stack.rdf.partition.spark.utils.InitialGraph
import org.apache.jena.graph.{Node, NodeVisitor}
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.sql.SparkSession

object QueryTest {
  def main(args: Array[String]): Unit = {
    val path = "src/main/resources/Clustering_sampledata.nt"
    val session = SparkSession.builder().master("local[*]").appName("Triple Pattern Test").getOrCreate()
    val graph = InitialGraph.applyAsString(session, path)

    val tp = TriplePattern[String,String]
    tp.srcAttr = "?user"
    tp.dstAttr = "http://twitter/user3"
    tp.attr = "http://twitter/follows"

    MatchSet.apply(graph,tp).groupByKey().collect().foreach(println(_))
  }
}
