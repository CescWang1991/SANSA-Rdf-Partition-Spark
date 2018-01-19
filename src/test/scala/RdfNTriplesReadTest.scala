import net.sansa_stack.rdf.spark.io.RDFReader
import org.apache.spark.sql.SparkSession

object RdfNTriplesReadTest {
  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
    println("RDF N-Triples Read Test")
    val session = SparkSession.builder
      .master("local")
      .appName("Rdf N-Triples Read Test")
      .getOrCreate()
    val rdfReader = new RDFReader()(session)
    val input = rdfReader.load(session,inputFile)
    input.map(triple=>triple.toString).foreach(println(_))  //org.apache.jena.riot.RiotException: [line: 1, col: 1 ]
  }
}
