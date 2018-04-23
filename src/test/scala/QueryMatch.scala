import net.sansa_stack.rdf.query.graph.jena.{BasicGraphPattern, SparqlParser}
import net.sansa_stack.rdf.query.graph.matching.{GenerateSolutionMappings, MatchSet}
import net.sansa_stack.rdf.spark.graph.LoadGraph
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.jena.graph.Node
import org.apache.spark.sql.SparkSession

object QueryMatch {
  def main(args: Array[String]): Unit = {
    val spPath = "src/resources/Sparql/Aggregate.txt"
    val ntPath = "src/resources/Rdf/Clustering_sampledata.nt"
    val session = SparkSession.builder().master("local[*]").getOrCreate()

    val sp = new SparqlParser(spPath)
    sp.OpVisitorWalker()
    val bgp = BasicGraphPattern(sp.getElementTriples, session.sparkContext)
    val graph = LoadGraph.apply (NTripleReader.load (session, ntPath))

    //bgp.tripleRDD.collect().foreach(println(_))
    //graph.triplets.collect().foreach(println(_))
    val solutionMapping = GenerateSolutionMappings.run[Node, Node](graph, bgp.triplePatterns, session)
    solutionMapping.foreach(println(_))

    //val ms = new MatchSet[Node, Node](graph, bgp.triplePatterns.collect(), session)
    //ms.matchCandidateSet.collect().foreach(println(_))
    //println("------------------------------------")
    //ms.validateLocalMatchSet(ms.matchCandidateSet).collect().foreach(println(_))
    //val matchSet =
    //ms.validateRemoteMatchSet(ms.validateLocalMatchSet(ms.matchCandidateSet))//.collect().foreach(println(_))
  }
}
