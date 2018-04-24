package net.sansa_stack.rdf.query.graph.jena.patternOp

import net.sansa_stack.rdf.query.graph.jena.BasicGraphPattern
import net.sansa_stack.rdf.query.graph.matching.GenerateSolutionMappings
import org.apache.jena.graph.Node
import org.apache.jena.sparql.core.BasicPattern
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

/**
  * Class that execute SPARQL UNION operations
  * @param bgp Basic Pattern for union
  */
class UnionOp(bgp: BasicPattern) extends PatternOp {

  private val tag = "UNION"

  override def execute(input: Array[Map[Node, Node]],
                       graph: Graph[Node, Node],
                       session: SparkSession): Array[Map[Node, Node]] = {
    val union = GenerateSolutionMappings.run[Node, Node](graph,
      BasicGraphPattern(bgp.toIterator, session.sparkContext).triplePatterns,
      session)
    input ++ union
  }

  override def getTag: String = { tag }
}
