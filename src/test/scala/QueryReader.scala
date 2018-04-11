import net.sansa_stack.rdf.query.graph.jena.{BasicGraphPattern, SparqlParser}
import net.sansa_stack.rdf.query.graph.matching._
import net.sansa_stack.rdf.query.graph.util.TriplePattern
import net.sansa_stack.rdf.spark.graph.LoadGraph
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.jena.graph.Node
import org.apache.jena.query.QueryFactory
import org.apache.spark.sql.SparkSession
import org.apache.jena.sparql.syntax.ElementFilter

import scala.collection.JavaConversions._

object QueryReader {
  def main(args: Array[String]): Unit = {
    val spPath = "src/resources/Sparql/QueryFilter.txt"
    val ntPath = "src/resources/Clustering_sampledata.nt"
    val session = SparkSession.builder().master("local[*]").getOrCreate()

    val sp = new SparqlParser(spPath)
    sp.OpVisitorWalker()
    val bgp = BasicGraphPattern(sp.getElementTriples, session.sparkContext)
    bgp.tripleRDD.foreach(println(_))
    val graph = LoadGraph.apply (NTripleReader.load (session, ntPath))

    val solutionMapping = GenerateSolutionMappings.run[Node, Node](graph, bgp.triplePatterns, session)
    //solutionMapping.foreach(println(_))

    val ops = sp.getGroupOp
    ops.foreach(op => op.execute())
  }
}
