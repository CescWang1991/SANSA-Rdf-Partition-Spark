package net.sansa_stack.rdf.query.graph.jena

import net.sansa_stack.rdf.query.graph.jena.graphOp._
import net.sansa_stack.rdf.query.graph.jena.patternOp.{PatternNegate, PatternOp, PatternOptional, PatternUnion}
import org.apache.jena.query.{Query, QueryFactory}
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.algebra.{Algebra, Op, OpVisitorBase, OpWalker}
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.expr.{E_Exists, E_NotExists}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Read sparql query from a file and convert to Op expressions.
  *
  * @param path path to sparql query file
  *
  * @author Zhe Wang
  */
class SparqlParser(path: String, op: Op) extends OpVisitorBase with Serializable {

  def this(path: String) {
    this(path, Algebra.compile(QueryFactory.create(Source.fromFile(path).mkString)))
  }

  def this(op: Op) {
    this("", op)
  }

  private val elementTriples = ArrayBuffer[Triple]()
  private val ops = new mutable.Queue[Ops]()

  OpWalker.walk(op, this)

  override def visit(opBGP: OpBGP): Unit = {
    println("opBGP: "+opBGP)
    val triples = opBGP.getPattern.toList
    for (triple <- triples) {
      elementTriples += triple
    }
  }

  override def visit(opDistinct: OpDistinct): Unit = {
    println("opDistinct: "+opDistinct)
    ops.enqueue(new GraphDistinct)
  }

  override def visit(opExtend: OpExtend): Unit = {
    println("opExtend: "+opExtend)
    ops.enqueue(new GraphExtend(opExtend))
  }

  override def visit(opFilter: OpFilter): Unit = {
    println("opFilter: "+opFilter)
    opFilter.getExprs.foreach{
      // Add triple pattern in filter expression EXISTS to elementTriples
      case e: E_Exists => val triples = e.getGraphPattern.asInstanceOf[OpBGP].getPattern
        for(triple <- triples) {
          elementTriples += triple
        }
      case e: E_NotExists => val triples = e.getGraphPattern.asInstanceOf[OpBGP].getPattern
        for(triple <- elementTriples){
          triples.add(triple)
        }
        ops.enqueue(new PatternNegate(triples))
      case other => ops.enqueue(new GraphFilter(other))
    }
  }

  override def visit(opGroup: OpGroup): Unit = {
    println("opGroup: "+opGroup)
    ops.enqueue(new GraphGroup(opGroup))
  }

  override def visit(opLeftJoin: OpLeftJoin): Unit = {
    println("opLeftJoin: "+opLeftJoin)
    val sp = new SparqlParser(opLeftJoin.getRight)
    elementTriples --= sp.getElementTriples
    ops.enqueue(new PatternOptional(sp.getElementTriples.toIterator, opLeftJoin.getExprs))
  }

  override def visit(opMinus: OpMinus): Unit = {
    println("opMinus: "+opMinus)
    val triples = opMinus.getRight.asInstanceOf[OpBGP].getPattern
    ops.enqueue(new PatternNegate(triples))
    val sp = new SparqlParser(opMinus.getRight)
    elementTriples --= sp.getElementTriples
  }

  override def visit(opOrder: OpOrder): Unit = {
    println("opOrder: "+opOrder)
    ops.enqueue(new GraphOrder(opOrder))
  }

  override def visit(opProject: OpProject): Unit = {
    println("opProject: "+opProject)
    ops.enqueue(new GraphProject(opProject))
  }

  override def visit(opReduced: OpReduced): Unit = {
    println("opReduced: "+opReduced)
    ops.enqueue(new GraphReduced)
  }

  override def visit(opSlice: OpSlice): Unit = {
    println("opSlice: "+opSlice)
    ops.enqueue(new GraphSlice(opSlice))
  }

  override def visit(opUnion: OpUnion): Unit = {
    println("opUnion: "+opUnion)
    val sp = new SparqlParser(opUnion.getRight)
    elementTriples --= sp.getElementTriples
    sp.getOps.foreach(op => ops.dequeueFirst {
      case e: GraphFilter => e.getExpr.equals(op.asInstanceOf[GraphFilter].getExpr)
      case _ => false
    })
    ops.enqueue(new PatternUnion(sp.getElementTriples.toIterator, sp.getOps))
  }

  def getOps: mutable.Queue[Ops] = {
    ops
  }

  def getElementTriples: ArrayBuffer[Triple] = {
    elementTriples
  }
}
