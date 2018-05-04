package net.sansa_stack.rdf.query.graph.jena.util

import net.sansa_stack.rdf.query.graph.matching.util.TriplePattern
import org.apache.jena.graph._
import org.apache.spark.graphx.EdgeTriplet

class TriplePatternNode(private val s: Node,
                        private val p: Node,
                        private val o: Node) extends Serializable {

  def this(triple: Triple) = {
    this(triple.getSubject, triple.getPredicate, triple.getObject)
  }

  def this(tp: TriplePattern[Node, Node]) = {
    this(tp.srcAttr, tp.attr, tp.dstAttr)
  }

  def getSubject: Node = {
    s
  }

  def getSubjectIsVariable: Boolean = {
    s.isVariable
  }

  def getPredicate: Node = {
    p
  }

  def getPredicateIsVariable: Boolean = {
    p.isVariable
  }

  def getObject: Node = {
    o
  }

  def getObjectIsVariable: Boolean = {
    o.isVariable
  }

  def isFulfilledByTriplet(triplet: EdgeTriplet[Node, Node]): Boolean = {
    checkQueryPattern(s, triplet.srcAttr) && checkQueryPattern(p, triplet.attr) && checkQueryPattern(o, triplet.dstAttr)
  }

  private def checkQueryPattern(pattern: Node, target: Node): Boolean = {
    if(pattern.isVariable){
      true
    } else {
      pattern.equals(target)
    }
  }

  def getVariable: Array[Node] = {
    Array[Node](s, p, o).filter(node => node.isVariable)
  }

  def compares(obj: TriplePatternNode): Boolean = {
    this.s.equals(obj.s) && this.p.equals(obj.p) && this.o.equals(obj.o)
  }

  override def toString: String = { s.toString+" "+p.toString+" "+o.toString+" ." }

  override def hashCode(): Int = {
    s.hashCode() + p.hashCode() + o.hashCode()
  }

  override def equals(obj: scala.Any): Boolean = {
    this.hashCode() == obj.hashCode()
  }
}
