package net.sansa_stack.rdf.query.graph.jena

import net.sansa_stack.rdf.query.graph.jena.graphOp._
import net.sansa_stack.rdf.query.graph.jena.patternOp.{NegateOp, OptionalOp, PatternOp, UnionOp}
import org.apache.jena.query.QueryFactory
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.algebra.{Algebra, Op, OpVisitorBase, OpWalker}
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.expr.{E_Exists, E_NotExists}

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
  private var graphOps = mutable.Queue[GraphOp]()
  private var patternOps = mutable.Queue[PatternOp]()
  private var elementTriples = Array[Triple]()

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
      elementTriples +:= triple
    }
  }

  override def visit(opDistinct: OpDistinct): Unit = {
    println("opDistinct: "+opDistinct)
    graphOps += new GraphDistinct
  }

  override def visit(opExtend: OpExtend): Unit = {
    println("opExtend: "+opExtend)
    graphOps += new GraphExtend(opExtend)
  }

  override def visit(opFilter: OpFilter): Unit = {
    println("opFilter: "+opFilter)
    opFilter.getExprs.foreach{
      // Add triple pattern in filter expression EXISTS to elementTriples
      case e: E_Exists => val triples = e.getGraphPattern.asInstanceOf[OpBGP].getPattern
        for(triple <- triples) {
          elementTriples +:= triple
        }
      case e: E_NotExists => val triples = e.getGraphPattern.asInstanceOf[OpBGP].getPattern
        for(triple <- elementTriples){
          triples.add(triple)
        }
        patternOps += new NegateOp(triples)
      case _ => graphOps += new GraphFilter(opFilter)
    }
  }

  override def visit(opGroup: OpGroup): Unit = {
    println("opGroup: "+opGroup)
    graphOps += new GraphGroup(opGroup)
  }

  override def visit(opLeftJoin: OpLeftJoin): Unit = {
    println("opLeftJoin: "+opLeftJoin)
    val triples = opLeftJoin.getRight.asInstanceOf[OpBGP].getPattern
    patternOps += new OptionalOp(triples)
    for(triple <- triples){
      elementTriples = elementTriples.filterNot(t => t.equals(triple))
    }
  }

  override def visit(opMinus: OpMinus): Unit = {
    println("opMinus: "+opMinus)
    val triples = opMinus.getRight.asInstanceOf[OpBGP].getPattern
    patternOps += new NegateOp(triples)
    for(triple <- triples){
      elementTriples = elementTriples.filterNot(t => t.equals(triple))
    }
  }

  override def visit(opOrder: OpOrder): Unit = {
    println("opOrder: "+opOrder)
    graphOps += new GraphOrder(opOrder)
  }

  override def visit(opProject: OpProject): Unit = {
    println("opProject: "+opProject)
    graphOps += new GraphProject(opProject)
  }

  override def visit(opReduced: OpReduced): Unit = {
    println("opReduced: "+opReduced)
    graphOps += new GraphReduced
  }

  override def visit(opSlice: OpSlice): Unit = {
    println("opSlice: "+opSlice)
    graphOps += new GraphSlice(opSlice)
  }

  override def visit(opUnion: OpUnion): Unit = {
    println("opUnion: "+opUnion)
    val triples = opUnion.getRight.asInstanceOf[OpBGP].getPattern
    patternOps += new UnionOp(triples)
    for(triple <- triples){
      elementTriples = elementTriples.filterNot(t => t.equals(triple))
    }
  }

  def getGraphOps: mutable.Queue[GraphOp] = {
    graphOps
  }

  def getPatternOps: mutable.Queue[PatternOp] = {
    patternOps
  }

  def getElementTriples: Iterator[Triple] = {
    elementTriples.toIterator
  }
}
