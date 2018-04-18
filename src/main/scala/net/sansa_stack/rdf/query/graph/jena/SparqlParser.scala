package net.sansa_stack.rdf.query.graph.jena

import net.sansa_stack.rdf.query.graph.jena.graphOp._
import org.apache.jena.query.QueryFactory
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.algebra.{Algebra, Op, OpVisitorBase, OpWalker}
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.expr.{E_Exists, E_NotExists}
import org.apache.jena.sparql.syntax.ElementTriplesBlock

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source

/**
  * Read sparql query from a file and convert to an Op expression.
  *
  * @param path path to sparql query file
  *
  * @author Zhe Wang
  */
class SparqlParser(path: String) extends OpVisitorBase with Serializable {

  private val query = QueryFactory.create(Source.fromFile(path).mkString)
  private val op = Algebra.compile(query)
  private var groupOp = mutable.Queue[GraphOp]()
  private val elementTriples = new ElementTriplesBlock()

  def OpVisitorWalker(): Unit = {
    OpWalker.walk(op, this)
  }

  override def visit(opBGP: OpBGP): Unit = {
    val triples = opBGP.getPattern.toList
    for (triple <- triples) {
      elementTriples.addTriple(triple)
    }
  }

  override def visit(opFilter: OpFilter): Unit = {
    opFilter.getExprs.foreach{
      // Add triple pattern in filter expression EXISTS to elementTriples
      case e: E_Exists => OpWalker.walk(e.getGraphPattern, this)
      case e: E_NotExists => OpWalker.walk(e.getGraphPattern, this)
      case _ => groupOp += new GraphFilter(opFilter)
    }
  }

  override def visit(opProject: OpProject): Unit = {
    groupOp += new GraphProject(opProject)
  }

  def getGroupOp: mutable.Queue[GraphOp] = {
    groupOp
  }

  def getElementTriples: Iterator[Triple] = {
    elementTriples.patternElts().toIterator
  }
}

/*object SparqlParser {
  def asQuery(op: Op, dialect: Dialect)
}*/
