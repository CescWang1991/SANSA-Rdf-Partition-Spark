package net.sansa_stack.rdf.query.graph.jena

import net.sansa_stack.rdf.query.graph.jena.graphOp._
import org.apache.jena.query.QueryFactory
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.algebra.{Algebra, OpVisitorBase, OpWalker}
import org.apache.jena.sparql.algebra.op._
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
    groupOp += new GraphFilter(opFilter)
  }

  def getGroupOp: mutable.Queue[GraphOp] = {
    groupOp
  }

  def getElementTriples: Iterator[Triple] = {
    elementTriples.patternElts().toIterator
  }
}
