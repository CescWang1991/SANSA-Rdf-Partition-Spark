import net.sansa_stack.rdf.partition.spark.utils.InitialGraph
import net.sansa_stack.rdf.query.graph.`match`.{GenerateSolutionMappings, MatchSet}
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.sql.SparkSession

object MatchTest {
  def main(args: Array[String]): Unit = {
    val path = "src/main/resources/s2x.nt"
    val session = SparkSession.builder().master("local[*]").appName("Triple Pattern Test").getOrCreate()
    val graph = InitialGraph.applyAsString(session, path)

    //val ms = new MatchSet(graph,tpList,session)
    //ms.matchCandidateSet.collect().foreach(println(_))
    //val localSet = ms.validateLocalMatchSet(ms.matchCandidateSet)//.collect().foreach(println(_))
    //ms.validateRemoteMatchSet(localSet).collect().foreach(println(_))
    //GenerateSolutionMappings.run(ms).foreach(println(_))
  }
}