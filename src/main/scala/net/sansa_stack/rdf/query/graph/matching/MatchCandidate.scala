package net.sansa_stack.rdf.query.graph.matching

import MatchCandidate.vertexType
import net.sansa_stack.rdf.query.graph.matching.util.TriplePattern
import org.apache.spark.graphx.{EdgeTriplet, VertexId}

import scala.reflect.ClassTag

/**
  * A match candidate of a target triple is a map of a triple pattern to the target triple.
  *
  * @param tp triple pattern to match.
  * @param triple target triple.
  * @param vt determine the type of vertex(subject, object) to return the match candidate.
  *
  * @tparam VD the type of the vertex attribute.
  * @tparam ED the type of the edge attribute.
  *
  * @author Zhe Wang
  */
class MatchCandidate[VD: ClassTag, ED: ClassTag](
    val triple: EdgeTriplet[VD,ED],
    val tp: TriplePattern[VD,ED],
    vt: vertexType) extends Serializable {

  val isMatch: Boolean = tp.isFulfilledByTriplet(triple)
  def vertex: (VertexId, VD) = {
    vt match {
      case MatchCandidate.s => (triple.srcId, triple.srcAttr)
      case MatchCandidate.o => (triple.dstId, triple.dstAttr)
    }
  }
  def variable: VD = {
    vt match {
      case MatchCandidate.s => tp.srcAttr
      case MatchCandidate.o => tp.dstAttr
    }
  }
  val isVar: Boolean = TriplePattern.isVariable(variable)
  val mapping: Map[VD,VD] = Map(tp.srcAttr->triple.srcAttr, tp.dstAttr->triple.dstAttr)

  override def toString: String = (vertex, variable, mapping, tp).toString
}

object MatchCandidate extends Enumeration {
  type vertexType = Value
  val s,o = Value
}
