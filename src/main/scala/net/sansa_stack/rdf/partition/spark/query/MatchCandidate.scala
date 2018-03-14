package net.sansa_stack.rdf.partition.spark.query

import org.apache.spark.graphx.EdgeTriplet

/**
  * A match candidate of a target triple is a map of a triple pattern to the target triple
  *
  * @param tp triple pattern to match
  * @param triple target triple
  *
  * @tparam VD the type of the vertex attribute.
  * @tparam ED the type of the edge attribute
  *
  * @author Zhe Wang
  */
class MatchCandidate[VD,ED](val triple: EdgeTriplet[VD,ED], val tp: TriplePattern[VD,ED]) extends Serializable {

  val isMatch: Boolean = tp.isFulfilledByTriplet(triple)
  val subjectMap = Map(tp.srcAttr->triple.srcAttr)
  val objectMap = Map(tp.dstAttr->triple.dstAttr)

  override def toString: String = (subjectMap, objectMap, triple.attr).toString
}
