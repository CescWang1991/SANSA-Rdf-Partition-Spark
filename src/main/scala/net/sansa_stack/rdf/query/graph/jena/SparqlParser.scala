package net.sansa_stack.rdf.query.graph.jena

import net.sansa_stack.rdf.query.graph.util.graphOp._
import org.apache.jena.query.QueryFactory
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.algebra.{Algebra, Op, OpVisitorBase, OpWalker}
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.syntax.{ElementGroup, ElementTriplesBlock}

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
class SparqlParser(path: String) extends OpVisitorBase{

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

  override def visit(opDistinct: OpDistinct): Unit = {
    groupOp += new GraphDistinct(opDistinct)
  }

  override def visit(opUnion: OpUnion): Unit = {
    groupOp += new GraphUnion(opUnion)
  }

  override def visit(opOrder: OpOrder): Unit = {
    groupOp += new GraphOrder(opOrder)
  }

  def getGroupOp: mutable.Queue[GraphOp] = {
    groupOp
  }

  def getElementTriples: Iterator[Triple] = {
    elementTriples.patternElts().toIterator
  }
}