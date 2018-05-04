import net.sansa_stack.rdf.query.graph.jena.util.{BasicGraphPattern, MatchCandidateNode, MatchSetNode, ResultMapping}
import net.sansa_stack.rdf.query.graph.jena.SparqlParser
import net.sansa_stack.rdf.spark.graph.LoadGraph
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.sql.SparkSession

object MatchNodeTest {
  def main(args: Array[String]): Unit = {
    val spPath = "src/resources/Sparql/SelectProject.txt"
    val ntPath = "src/resources/Rdf/Clustering_sampledata.nt"
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val sp = new SparqlParser(spPath)
    val graph = LoadGraph.apply (NTripleReader.load (session, ntPath))
    val bgp = new BasicGraphPattern(sp.getElementTriples.toIterator)
    val ms = new MatchSetNode(graph, bgp, session)
    //ms.matchCandidateSet.collect().foreach(println(_))
    //ms.validateLocalMatchSet(ms.matchCandidateSet).collect().foreach(println(_))
    //ms.validateRemoteMatchSet(ms.validateLocalMatchSet(ms.matchCandidateSet)).collect().foreach(println(_))
    ResultMapping.run(graph, bgp, session).foreach(println(_))
  }
}
