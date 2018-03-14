package net.sansa_stack.rdf.partition.spark.query

import org.apache.spark.graphx.EdgeTriplet

/**
  * A match set is the set of all candidates of the target triple matching the basic graph pattern
  *
  * @param triple the target triple
  * @param bgp basic graph pattern to match
  *
  * @tparam VD the type of the vertex attribute.
  * @tparam ED the type of the edge attribute
  */
class MatchSet[VD,ED](val triple: EdgeTriplet[VD,ED], bgp: BasicGraphPattern[VD,ED]) extends Serializable {

  val set: Array[MatchCandidate[VD,ED]] = bgp.triplePattern.map { tp =>
    new MatchCandidate(triple, tp)
  }.collect().filter(mc => mc.isMatch)

}
