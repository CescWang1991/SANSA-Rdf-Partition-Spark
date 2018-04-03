import java.io.ByteArrayInputStream

import net.sansa_stack.rdf.partition.spark.utils.InitialGraph
import net.sansa_stack.rdf.query.graph.parser.BasicGraphPattern
import org.apache.spark.sql.SparkSession

object TripleReaderTest {
  def main(args: Array[String]): Unit = {
    val ntPath = "src/resources/Clustering_sampledata.nt"
    val sqPath = "src/resources/SparqlQuery.txt"
    val session = SparkSession.builder().master("local[*]").appName("Triple Reader Test").getOrCreate()
    val graph = InitialGraph.applyAsString(session, ntPath)
    val bgp = new BasicGraphPattern(session.sparkContext, sqPath)
  }
}
