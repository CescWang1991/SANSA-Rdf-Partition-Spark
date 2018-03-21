import net.sansa_stack.rdf.partition.spark.query.{GenerateSolutionMappings, MatchCandidate, MatchSet, TriplePattern}
import net.sansa_stack.rdf.partition.spark.utils.InitialGraph
import org.apache.spark.sql.SparkSession

object QueryTest {

  def main(args: Array[String]): Unit = {
    //val path = "src/main/resources/s2x.nt"
    val path = "src/main/resources/Clustering_sampledata.nt"
    val session = SparkSession.builder().master("local[*]").appName("Triple Pattern Test").getOrCreate()
    val graph = InitialGraph.applyAsString(session, path)

    val tp1 = TriplePattern[String,String]
    tp1.srcAttr = "?A"
    tp1.srcId = 0L
    tp1.attr = "http://twitter/follows"
    tp1.dstAttr = "http://twitter/user1"
    tp1.dstId = 1L
    val tp2 = TriplePattern[String,String]
    tp2.srcAttr = "http://twitter/user1"
    tp2.srcId = 1L
    tp2.attr = "http://twitter/follows"
    tp2.dstAttr = "?B"
    tp2.dstId = 2L
    val tp3 = TriplePattern[String,String]
    tp3.srcAttr = "?B"
    tp3.srcId = 2L
    tp3.attr = "http://twitter/follows"
    tp3.dstAttr = "http://twitter/user3"
    tp2.dstId = 3L
    val tpList = Array(tp1,tp2,tp3)

    val ms = new MatchSet(graph,tpList,session)
    //ms.matchCandidateSet.collect().foreach(println(_))
    //val localSet = ms.validateLocalMatchSet(ms.matchCandidateSet)//.collect().foreach(println(_))
    //ms.validateRemoteMatchSet(localSet).collect().foreach(println(_))
    GenerateSolutionMappings.run(ms).foreach(println(_))
  }
}
