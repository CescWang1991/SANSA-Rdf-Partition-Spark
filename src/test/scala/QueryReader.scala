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
    val spPath = "src/resources/Sparql/QueryFilter.txt"
    val ntPath = "src/resources/Clustering_sampledata.nt"
    val session = SparkSession.builder().master("local[*]").getOrCreate()

    val sp = new SparqlParser(spPath)
    sp.OpVisitorWalker()
    val bgp = BasicGraphPattern(sp.getElementTriples, session.sparkContext)
    val graph = LoadGraph.apply (NTripleReader.load (session, ntPath))

    val solutionMapping = GenerateSolutionMappings.run[Node, Node](graph, bgp.triplePatterns, session)
    //solutionMapping.foreach(println(_))
    //println(solutionMapping.head.map{ case(k, v) => NodeValue.makeNode(v) }.toList)
    val exprFilter = sp.filter.getExprs.head

  }
}
