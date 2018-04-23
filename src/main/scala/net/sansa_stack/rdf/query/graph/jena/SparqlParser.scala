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
    println("Op: "+op)
    OpWalker.walk(op, this)
  }

  override def visit(opAssign: OpAssign): Unit = {
    println("opAssign: "+opAssign)
  }

  override def visit(opBGP: OpBGP): Unit = {
    println("opBGP: "+opBGP)
    val triples = opBGP.getPattern.toList
    for (triple <- triples) {
      elementTriples.addTriple(triple)
    }
  }

  override def visit(opDistinct: OpDistinct): Unit = {
    println("opDistinct: "+opDistinct)
    groupOp += new GraphDistinct
  }

  override def visit(opExtend: OpExtend): Unit = {
    println("opExtend: "+opExtend)
    groupOp += new GraphExtend(opExtend)
  }

  override def visit(opFilter: OpFilter): Unit = {
    println("opFilter: "+opFilter)
    opFilter.getExprs.foreach{
      // Add triple pattern in filter expression EXISTS to elementTriples
      case e: E_Exists => OpWalker.walk(e.getGraphPattern, this)
      case e: E_NotExists => OpWalker.walk(e.getGraphPattern, this)
      case _ => groupOp += new GraphFilter(opFilter)
    }
  }

  override def visit(opGroup: OpGroup): Unit = {
    println("opGroup: "+opGroup)
    groupOp += new GraphGroup(opGroup)
  }

  override def visit(opOrder: OpOrder): Unit = {
    println("opOrder: "+opOrder)
    groupOp += new GraphOrder(opOrder)
  }

  override def visit(opProject: OpProject): Unit = {
    println("opProject: "+opProject)
    groupOp += new GraphProject(opProject)
  }

  override def visit(opReduced: OpReduced): Unit = {
    println("opReduced: "+opReduced)
    groupOp += new GraphReduced
  }

  override def visit(opSlice: OpSlice): Unit = {
    println("opSlice: "+opSlice)
    groupOp += new GraphSlice(opSlice)
  }

  def getGroupOp: mutable.Queue[GraphOp] = {
    groupOp
  }

  def getElementTriples: Iterator[Triple] = {
    elementTriples.patternElts().toIterator
  }
}
