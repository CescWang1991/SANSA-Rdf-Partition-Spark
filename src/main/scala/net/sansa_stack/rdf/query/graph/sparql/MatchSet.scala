package net.sansa_stack.rdf.query.graph.sparql

import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import MatchCandidate._
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks._
import scala.reflect.ClassTag

/**
  * A match set of vertex v is the set of all candidates of the target vertex matching triple patterns
  * from basic graph pattern.
  *
  * @param graph the target rdf graph
  * @param tpList list of triple patterns to match
  * @param session spark session
  * @tparam VD the type of the vertex attribute.
  * @tparam ED the type of the edge attribute
  *
  * @author Zhe Wang
  */
class MatchSet[VD: ClassTag, ED: ClassTag](
    val graph: Graph[VD,ED],
    val tpList: Array[TriplePattern[VD,ED]],
    session: SparkSession) extends Serializable {

  def matchCandidateSet: RDD[MatchCandidate[VD,ED]] = {
    val subjectMatchSet = graph.triplets.flatMap{ triplet =>
      tpList.map{ tp => new MatchCandidate(triplet, tp, MatchCandidate.s) }.filter(_.isMatch).filter(_.isVar)
    }
    val objectMatchSet = graph.triplets.flatMap{ triplet =>
      tpList.map{ tp => new MatchCandidate(triplet, tp, MatchCandidate.o) }.filter(_.isMatch).filter(_.isVar)
    }
    subjectMatchSet.++(objectMatchSet)
  }

  /**
    * Conform the validation of local match sets. Filter match sets which has local match.
    *
    * @param matchSet match candidate set of all vertices in rdf graph
    * @return match candidate set after filter.
    */
  def validateLocalMatchSet(matchSet: RDD[MatchCandidate[VD,ED]]): RDD[MatchCandidate[VD,ED]] = {
    val broadcast = session.sparkContext.broadcast(matchSet.collect())
    matchSet.filter{ mc =>  //foreach matchC1 2 v.matchS do
      var exists = true
      breakable{
        tpList.filterNot(_.equals(mc.triple)).foreach { tp => //foreach tp 2 BGP != matchC1.tp do
          if (tp.getVariable.contains(mc.variable)) {
            val localMatchSet = broadcast.value.filter(_.vertex.equals(mc.vertex))
            val numOfExist = localMatchSet.count{ mc2 =>
              mc2.tp.equals(tp) && mc2.variable.equals(mc.variable) && compatible(mc2.mapping, mc.mapping)
            }
            if (numOfExist == 0) {
              exists = false
              break
            }
          }
        }
      }
      exists
    }
  }

  /**
    * Conform the validation of remote match sets. Filter match sets which has remote match.
    * @param matchSet match candidate set of all vertices in rdf graph.
    * @return match candidate set after filter.
    */
  def validateRemoteMatchSet(matchSet: RDD[MatchCandidate[VD,ED]]): RDD[MatchCandidate[VD,ED]] = {
    val broadcast = session.sparkContext.broadcast(matchSet.collect())
    val neighborBroadcast = session.sparkContext.broadcast(graph.ops.collectNeighbors(EdgeDirection.Either).collect())
    matchSet.filter { mc => //foreach matchC1 2 v.matchS do
      var exists = true
      breakable{
        val var2 = mc.tp.getVariable.filterNot(_.equals(mc.variable))   //?var2 <- { vars(matchC1.tp) \ matchC1.var }
        if(var2.length != 0){    //if ?var2 != None then
          val neighbors = neighborBroadcast.value.filter{ case(vid,_) => vid == mc.vertex._1 }.head._2
          val remoteMatchSet = broadcast.value.filter(mc1 => neighbors.contains(mc1.vertex))
          val numOfExist = remoteMatchSet.count{ mc2 =>
            mc2.variable.equals(var2.head) && mc2.tp.equals(mc.tp) && compatible(mc2.mapping, mc.mapping)
          }
          if (numOfExist == 0) {
            exists = false
            break
          }
        }
      }
      exists
    }
  }

  lazy val matchTriplePattern: RDD[TriplePattern[VD,ED]] = matchCandidateSet.map(_.tp).distinct()

  lazy val matchVertices: RDD[(VertexId, VD)] = matchCandidateSet.map(_.vertex).distinct()

  lazy val matchVariable: RDD[VD] = matchCandidateSet.map(_.variable).distinct()

  private def compatible(map1: Map[VD,VD], map2: Map[VD,VD]): Boolean = {
    if(map1.keys.equals(map2.keys)){
      map1.equals(map2)
    }
    else{
      if(map1.keys.head.equals(map2.keys.head)) { map1.values.head.equals(map2.values.head) }
      else if(map1.keys.head.equals(map2.keys.last)) { map1.values.head.equals(map2.values.last) }
      else if(map1.keys.last.equals(map2.keys.head)) { map1.values.last.equals(map2.values.head) }
      else if(map1.keys.last.equals(map2.keys.last)) { map1.values.last.equals(map2.values.last) }
      else { true }
    }
  }
}
