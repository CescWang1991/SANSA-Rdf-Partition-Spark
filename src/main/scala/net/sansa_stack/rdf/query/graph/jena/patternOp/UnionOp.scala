package net.sansa_stack.rdf.query.graph.jena.patternOp

import net.sansa_stack.rdf.query.graph.jena.{BasicGraphPattern, ExprParser}
import net.sansa_stack.rdf.query.graph.matching.GenerateSolutionMappings
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.sparql.algebra.Op
import org.apache.jena.sparql.algebra.op.{OpBGP, OpFilter, OpUnion}
import org.apache.jena.sparql.core.BasicPattern
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

/**
  * Class that execute SPARQL UNION operations
  * @param bgp Operator for union
  */
class UnionOp(bgp: Array[Triple]) extends PatternOp {

  private val tag = "UNION"
  //private val bgp = op.getRight.asInstanceOf[OpBGP].getPattern

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
