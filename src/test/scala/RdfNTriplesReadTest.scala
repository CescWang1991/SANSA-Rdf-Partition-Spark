import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.graph.LoadGraph
import org.apache.spark.sql.SparkSession

object RdfNTriplesReadTest {
  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
    val session = SparkSession.builder
      .master("local")
      .appName("Rdf N-Triples Read Test")
      .getOrCreate()
    println("Read a N-Triples File and Output as RDD[Triple]")
    val input = NTripleReader.load(session,inputFile)
    //input.map(triple=>triple.toString).foreach(println) //org.apache.jena.riot.RiotException: [line: 1, col: 41 ]

    println("Load RDD[Triple] and Output as org.apache.spark.graphX.graph")
    val graph = LoadGraph.apply(input)
    graph.triplets.foreach(println)
  }
}
