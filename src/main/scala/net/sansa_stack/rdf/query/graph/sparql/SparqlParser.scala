package net.sansa_stack.rdf.query.graph.sparql

import org.apache.jena.query.QueryFactory
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.algebra.{Algebra, Op, OpVisitorBase, OpWalker}
import org.apache.jena.sparql.algebra.op.OpBGP
import org.apache.jena.sparql.syntax.{ElementGroup, ElementTriplesBlock}
import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Read sparql query from a file and convert an Op expression.
  * @param path path to sparql query file
  *
  * @author Zhe Wang
  */
class SparqlParser(path: String) extends OpVisitorBase{

  private val query = QueryFactory.create(Source.fromFile(path).mkString)
  private val op = Algebra.compile(query)
  private val elementGroup = new ElementGroup()
  private val elementTriples = new ElementTriplesBlock()

  def OpVisitorWalker(): Unit = {
    OpWalker.walk(op, this)
  }

  override def visit(opBGP: OpBGP): Unit = {
    val triples = opBGP.getPattern.toList
    for (triple <- triples) {
      elementTriples.addTriple(triple)
    }
    elementGroup.addElement(elementTriples)
  }

  def getElementGroup: ElementGroup = {
    elementGroup
  }

  def getElementTriples: Iterator[Triple] = {
    elementTriples.patternElts().toIterator
  }
}
