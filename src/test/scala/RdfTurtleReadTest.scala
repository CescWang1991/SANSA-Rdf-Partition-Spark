import net.sansa_stack.rdf.spark.io.RDFReader
import org.apache.spark.sql.SparkSession

object RdfTurtleReadTest {
  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
    println("RDF Turtle Read Test")
    val session = SparkSession.builder
      .master("local")
      .appName("Rdf Turtle Read Test")
      .getOrCreate()
    val rdfReader = new RDFReader()(session)
    val input = rdfReader.load(session,inputFile)
    input.map(triple=>triple.toString).foreach(println(_))  //org.apache.jena.riot.RiotException: [line: 1, col: 1 ]
  }
}
