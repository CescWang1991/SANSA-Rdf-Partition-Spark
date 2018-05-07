package net.sansa_stack.rdf.query.graph.jena.patternOp

import net.sansa_stack.rdf.query.graph.jena.resultOp.ResultOp
import net.sansa_stack.rdf.query.graph.jena.util.{BasicGraphPattern, ResultMapping}
import net.sansa_stack.rdf.query.graph.jena.{ExprParser, Ops}
import net.sansa_stack.rdf.query.graph.matching.GenerateSolutionMappings
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.sparql.algebra.Op
import org.apache.jena.sparql.algebra.op.{OpBGP, OpFilter, OpUnion}
import org.apache.jena.sparql.core.BasicPattern
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Class that execute SPARQL UNION operations
  */
class PatternUnion(triples: Iterator[Triple], ops: mutable.Queue[Ops]) extends PatternOp {

  private val tag = "UNION"

  override def execute(input: Array[Map[Node, Node]],
                       graph: Graph[Node, Node],
                       session: SparkSession): Array[Map[Node, Node]] = {
    var union = ResultMapping.run(graph, new BasicGraphPattern(triples), session)
    ops.foreach(op => union = op.asInstanceOf[ResultOp].execute(union))
    input ++ union
  }

  override def getTag: String = { tag }
}
