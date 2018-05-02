import net.sansa_stack.rdf.query.graph.jena.resultOp._
import net.sansa_stack.rdf.query.graph.jena.patternOp.PatternOp
import net.sansa_stack.rdf.query.graph.jena.{BasicGraphPattern, ExprParser, SparqlParser}
import net.sansa_stack.rdf.query.graph.matching._
import net.sansa_stack.rdf.query.graph.matching.util.GenerateDescribeGraph
import net.sansa_stack.rdf.spark.graph.LoadGraph
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.jena.graph.Node
import org.apache.jena.query.{QueryFactory, QuerySolutionMap}
import org.apache.jena.sparql.algebra.{Algebra, OpAsQuery}
import org.apache.jena.sparql.algebra.op.{OpBGP, OpFilter}
import org.apache.jena.sparql.core.BasicPattern
import org.apache.jena.sparql.expr.NodeValue
import org.apache.spark.sql.SparkSession
import org.apache.jena.sparql.syntax.ElementFilter

import scala.collection.JavaConversions._
import scala.io.Source

object QueryReader {
  def main(args: Array[String]): Unit = {
    val spPath = "src/resources/BSBM/query11.txt"
    val ntPath = "src/resources/Rdf/Clustering_sampledata.nt"

    val sp = new SparqlParser(spPath)

    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val bgp = BasicGraphPattern(sp.getElementTriples.toIterator, session.sparkContext)
    val graph = LoadGraph.apply (NTripleReader.load (session, ntPath))
    val solutionMapping = GenerateSolutionMappings.run[Node, Node](graph, bgp.triplePatterns, session)
    solutionMapping.foreach(println(_))
    var intermediate = solutionMapping
    sp.getOps.foreach{
      case op: ResultOp => intermediate = op.execute(intermediate)
        println(op.getTag)
        intermediate.foreach(println(_))
      case op: PatternOp => intermediate = op.execute(intermediate, graph, session)
        println(op.getTag)
        intermediate.foreach(println(_))
    }
    val result = intermediate
    /*println("DESCRIBE")
    GenerateDescribeGraph.run(result, graph).triplets.collect().foreach(println(_))*/
  }
}
