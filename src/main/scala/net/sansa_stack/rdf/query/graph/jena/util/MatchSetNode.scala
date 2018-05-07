package net.sansa_stack.rdf.query.graph.jena.util

import net.sansa_stack.rdf.query.graph.matching.util.TriplePattern
import org.apache.jena.graph.Node
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag
import scala.util.control.Breaks._

/**
  * A match set of vertex v is the set of all candidates of the target vertex matching triple patterns
  * from basic graph pattern.
  *
  * @author Zhe Wang
  */
class MatchSetNode(val graph: Graph[Node, Node],
                   val bgp: BasicGraphPattern,
                   session: SparkSession) extends Serializable {

  private val triplePatterns = session.sparkContext.broadcast(bgp.triplePatterns)

  def matchCandidateSet: RDD[MatchCandidateNode] = {
    val subjectMatchSet = graph.triplets.flatMap{ triplet =>
      triplePatterns.value.map{ tp => new MatchCandidateNode(triplet, tp, MatchCandidate.s) }
        .filter(_.isMatch).filter(_.isVar)
    }
    val predicateMatchSet = graph.triplets.flatMap{ triplet =>
      triplePatterns.value.map{ tp => new MatchCandidateNode(triplet, tp, MatchCandidate.p) }
        .filter(_.isMatch).filter(_.isVar)
    }
    val objectMatchSet = graph.triplets.flatMap{ triplet =>
      triplePatterns.value.map{ tp => new MatchCandidateNode(triplet, tp, MatchCandidate.o) }
        .filter(_.isMatch).filter(_.isVar)
    }
    subjectMatchSet.++(predicateMatchSet).++(objectMatchSet)
  }

  /**
    * Conform the validation of local match sets. Filter match sets which has local match.
    *
    * matchSet match candidate set of all vertices in rdf graph
    * @return match candidate set after filter.
    */
  def validateLocalMatchSet(matchSet: RDD[MatchCandidateNode]): RDD[MatchCandidateNode] = {
    val broadcast = session.sparkContext.broadcast(matchSet.collect())
    val tpList = triplePatterns.value
    matchSet.filter{ mc =>  //foreach matchC1 2 v.matchS do
      var exists = true
      breakable{
        tpList.filterNot(_.equals(mc.pattern)).foreach { tp => //foreach tp 2 BGP != matchC1.tp do
          if (tp.getVariable.contains(mc.variable)) {
            val localMatchSet = broadcast.value.filter(_.vertex.equals(mc.vertex))
            val numOfExist = localMatchSet.count{ mc2 =>
              mc2.pattern.compares(tp) && mc2.variable.equals(mc.variable) && compatible(mc2.mapping, mc.mapping)
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
    * @return match candidate set after filter.
    */
  def validateRemoteMatchSet(matchSet: RDD[MatchCandidateNode]): RDD[MatchCandidateNode] = {
    val broadcast = session.sparkContext.broadcast(matchSet.collect())
    val neighborBroadcast = session.sparkContext.broadcast(graph.ops.collectNeighbors(EdgeDirection.Either).collect())
    matchSet.filter { mc => //foreach matchC1 2 v.matchS do
      var exists = true
      breakable{
        val var2 = mc.pattern.getVariable.filterNot(_.equals(mc.variable))   //?var2 <- { vars(matchC1.tp) \ matchC1.var }
        if(var2.length != 0){    //if ?var2 != None then
          val neighbors = {
            val neighorList = neighborBroadcast.value.filter{ case(vid, _) => vid == mc.vertex._1 }
            neighorList.length match {
              case 0 => Array[(VertexId, Node)]()
              case _ => neighorList.head._2
            }
          }
          val remoteMatchSet = broadcast.value.filter(mc1 =>
            neighbors.map(_._2).toSet.intersect(mc1.mapping.valuesIterator.toSet).nonEmpty)
          if(remoteMatchSet.length==0){
            break()
          }
          val numOfExist = remoteMatchSet.count{ mc2 =>
            mc2.variable.equals(var2.head) && mc2.pattern.compares(mc.pattern) && compatible(mc2.mapping, mc.mapping)
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

  private def compatible(map1: Map[Node,Node], map2: Map[Node,Node]): Boolean = {
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
