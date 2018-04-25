import net.sansa_stack.rdf.query.graph.jena.graphOp._
import net.sansa_stack.rdf.query.graph.jena.{BasicGraphPattern, ExprParser, SparqlParser}
import net.sansa_stack.rdf.query.graph.matching._
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
    val spPath = "src/resources/BSBM_Similar/query4.txt"
    val ntPath = "src/resources/Rdf/Clustering_sampledata.nt"

    val sp = new SparqlParser(spPath)

    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val bgp = BasicGraphPattern(sp.getElementTriples, session.sparkContext)
    val graph = LoadGraph.apply (NTripleReader.load (session, ntPath))
    /*val solutionMapping = GenerateSolutionMappings.run[Node, Node](graph, bgp.triplePatterns, session)
    solutionMapping.foreach(println(_))
    var intermediate = solutionMapping
    sp.getPatternOps.foreach { op =>
      println(op.getTag)
      intermediate = op.execute(intermediate, graph, session)
      intermediate.foreach(println(_))
    }
    sp.getGraphOps.foreach{op => println(op.getTag)
      intermediate = op.execute(intermediate)
      intermediate.foreach(println(_))}*/

    sp.getOps.foreach(op => println("TAG: "+op.getTag+"; TYPE: "+op.isInstanceOf[GraphOp]))
  }
}
