package net.sansa_stack.rdf.query.graph.jena

import net.sansa_stack.rdf.query.graph.jena.graphOp._
import net.sansa_stack.rdf.query.graph.jena.patternOp.{NegateOp, OptionalOp, PatternOp, UnionOp}
import org.apache.jena.query.{Query, QueryFactory}
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
class SparqlParser(path: String, op: Op) extends OpVisitorBase with Serializable {

  def this(path: String) {
    this(path, Algebra.compile(QueryFactory.create(Source.fromFile(path).mkString)))
  }

  def this(op: Op) {
    this("", op)
  }

  private var elementTriples = Array[Triple]()
  private var additionTriples = Array[Triple]()
  //private var graphOps = mutable.Queue[GraphOp]()
  //private var patternOps = mutable.Queue[PatternOp]()
  private val ops = new mutable.Queue[Ops]

  OpWalker.walk(op, this)

  override def visit(opAssign: OpAssign): Unit = {
    println("opAssign: "+opAssign)
  }

  private var bgpNum = 0    // count num of bgp in query

  override def visit(opBGP: OpBGP): Unit = {
    println("opBGP: "+opBGP)
    val triples = opBGP.getPattern.toList
    if(bgpNum == 0) {
      for (triple <- triples) {
        elementTriples +:= triple
      }
    } else {
      for (triple <- triples) {
        additionTriples +:= triple
      }
    }
    bgpNum += 1
  }

  override def visit(opDistinct: OpDistinct): Unit = {
    println("opDistinct: "+opDistinct)
    //graphOps += new GraphDistinct
    ops.enqueue(new GraphDistinct)
  }

  override def visit(opExtend: OpExtend): Unit = {
    println("opExtend: "+opExtend)
    //graphOps += new GraphExtend(opExtend)
    ops.enqueue(new GraphExtend(opExtend))
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
        ops.enqueue(new NegateOp(triples))  //patternOps += new NegateOp(triples)
      case _ => ops.enqueue(new GraphFilter(opFilter))  //graphOps += new GraphFilter(opFilter)
    }
  }

  override def visit(opGroup: OpGroup): Unit = {
    println("opGroup: "+opGroup)
    ops.enqueue(new GraphGroup(opGroup))  //graphOps += new GraphGroup(opGroup)
  }

  override def visit(opLeftJoin: OpLeftJoin): Unit = {
    println("opLeftJoin: "+opLeftJoin)
    ops.enqueue(new OptionalOp(opLeftJoin))  //patternOps += new OptionalOp(opLeftJoin)
    val triples = opLeftJoin.getRight.asInstanceOf[OpBGP].getPattern
    for(triple <- triples){
      elementTriples = dropFirstMatch(elementTriples, triple)
    }
  }

  override def visit(opMinus: OpMinus): Unit = {
    println("opMinus: "+opMinus)
    val triples = opMinus.getRight.asInstanceOf[OpBGP].getPattern
    ops.enqueue(new NegateOp(triples))     //patternOps += new NegateOp(triples)
    for(triple <- triples){
      elementTriples = dropFirstMatch(elementTriples, triple)
    }
  }

  override def visit(opOrder: OpOrder): Unit = {
    println("opOrder: "+opOrder)
    ops.enqueue(new GraphOrder(opOrder))  //graphOps += new GraphOrder(opOrder)
  }

  override def visit(opProject: OpProject): Unit = {
    println("opProject: "+opProject)
    ops.enqueue(new GraphProject(opProject))  //graphOps += new GraphProject(opProject)
  }

  override def visit(opReduced: OpReduced): Unit = {
    println("opReduced: "+opReduced)
    ops.enqueue(new GraphReduced)  //graphOps += new GraphReduced
  }

  override def visit(opSlice: OpSlice): Unit = {
    println("opSlice: "+opSlice)
    ops.enqueue(new GraphSlice(opSlice))  //graphOps += new GraphSlice(opSlice)
  }

  override def visit(opUnion: OpUnion): Unit = {
    println("opUnion: "+opUnion)
    ops.enqueue(new UnionOp(additionTriples))   //patternOps += new UnionOp(additionTriples)
    //println(opUnion.getRight.asInstanceOf[OpFilter].getExprs)
    /*val triples = opUnion.getRight.asInstanceOf[OpBGP].getPattern
    for(triple <- triples){
      elementTriples = dropFirstMatch(elementTriples, triple)
    }*/
  }

  /*def getGraphOps: mutable.Queue[GraphOp] = {
    graphOps
  }

  def getPatternOps: mutable.Queue[PatternOp] = {
    patternOps
  }*/

  def getOps: mutable.Queue[Ops] = {
    ops
  }

  def getElementTriples: Iterator[Triple] = {
    elementTriples.toIterator
  }

  private def dropFirstMatch(ls: Array[Triple], value: Triple): Array[Triple] = {
    val index = ls.indexOf(value)   //index = -1 if no match
    if(index < 0){
      ls
    } else if (index == 0) {
      ls.tail
    } else {
      val (a, b) = ls.splitAt(index)
      a ++ b.tail
    }
  }
}
