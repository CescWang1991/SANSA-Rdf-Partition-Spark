import net.sansa_stack.rdf.query.graph.jena.resultRddOp.ResultRddOp
import net.sansa_stack.rdf.query.graph.jena.resultOp.ResultOp
import net.sansa_stack.rdf.query.graph.jena.SparqlParser
import net.sansa_stack.rdf.query.graph.matching.GenerateSolutionMappings
import net.sansa_stack.rdf.query.graph.matching.util.ResultFactory
import net.sansa_stack.rdf.spark.graph.LoadGraph
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.jena.graph.Node
import org.apache.spark.sql.SparkSession

object ResultTest {
  def main(args: Array[String]): Unit = {
    val ntPath = "src/resources/Rdf/Clustering_sampledata.nt"
    val spPath = "src/resources/Sparql/SelectProject.txt"

    val session = SparkSession.builder()
      .master("local[*]")
      .appName("result test")
      .getOrCreate()
    val graph = LoadGraph.apply (NTripleReader.load (session, ntPath))
    val sp = new SparqlParser(spPath)
    //val bgp = BasicGraphPattern(sp.getElementTriples.toIterator, session.sparkContext)
    /*val solutionMapping = GenerateSolutionMappings.run[Node, Node](graph, bgp.triplePatterns, session)
    val result = ResultFactory.create(solutionMapping, session)
    var intermediate = result
    sp.getOps.foreach {
      case op: ResultRddOp => println(op.getTag)
        intermediate = op.execute(intermediate, session)
        intermediate.collect().foreach(result => println(result))
      case op: ResultOp => println(op.getTag)
    }*/
  }
}
