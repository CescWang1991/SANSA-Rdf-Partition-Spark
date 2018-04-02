import java.io.ByteArrayInputStream

import net.sansa_stack.rdf.partition.spark.utils.InitialGraph
import net.sansa_stack.rdf.query.graph.parser.Query
import org.apache.spark.sql.SparkSession
import org.apache.jena.graph.NodeFactory
import org.apache.jena.graph.Triple
import org.apache.jena.riot.{Lang, RDFDataMgr}

object TripleReaderTest {
  def main(args: Array[String]): Unit = {
    val ntPath = "src/resources/Clustering_sampledata.nt"
    val sqPath = "src/resources/SparqlQuery.txt"
    val session = SparkSession.builder().master("local[*]").appName("Triple Reader Test").getOrCreate()
    val graph = InitialGraph.applyAsString(session, ntPath)
    val expr = new Query(session.sparkContext, sqPath)
    val triple = expr.triplePatterns(0)
    println(triple)
    val t = "user <http://twitter/follows> <http://twitter/user1> ."
    println(RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(t.getBytes), Lang.NTRIPLES, null).next())
  }
}
