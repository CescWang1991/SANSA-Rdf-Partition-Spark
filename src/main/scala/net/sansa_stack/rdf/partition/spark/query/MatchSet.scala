package net.sansa_stack.rdf.partition.spark.query

import org.apache.spark.graphx.{EdgeTriplet, Graph}
import org.apache.spark.rdd.RDD

/**
  * A match set of vertex v is the set of all candidates of the target vertex matching triple patterns
  * from basic graph pattern.
  *
  */
object MatchSet extends Serializable {

  import MatchCandidate._

  /**
    *
    * @param graph the target rdf graph
    * @param tp basic graph pattern to match
    *
    * @tparam VD the type of the vertex attribute.
    * @tparam ED the type of the edge attribute
    */
  def apply[VD,ED](graph: Graph[VD,ED], tp: TriplePattern[VD,ED]) : RDD[(VD, matchMap[VD,ED])] = {
    val candidateSet = graph.triplets.map{ triplet =>
      new MatchCandidate(triplet, tp)
    }
    val subjectMatchSet = candidateSet.filter(candidate => candidate.isMatch).map(_.subjectMatch)
    val objectMatchSet = candidateSet.filter(candidate => candidate.isMatch).map(_.objectMatch)
    subjectMatchSet.++(objectMatchSet)
  }
}
