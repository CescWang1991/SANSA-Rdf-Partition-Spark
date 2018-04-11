package net.sansa_stack.rdf.query.graph.matching

import net.sansa_stack.rdf.query.graph.util.TriplePattern
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * Generate mappings from variables in sparql query to vertices in target rdf graph
  *
  * @author Zhe Wang
  */
object GenerateSolutionMappings {

  /**
    * run algorithm to generate solution mapping.
    *
    * @param rdfGraph rdf graph to query.
    * @param triplePattern collection of sparql triple patterns
    *
    * @tparam VD the type of the vertex attribute.
    * @tparam ED the type of the edge attribute.
    * @return basic graph pattern mapping.
    */
  def run[VD: ClassTag, ED: ClassTag](rdfGraph: Graph[VD, ED],
      triplePattern: RDD[TriplePattern[VD, ED]],
      session: SparkSession): Array[Map[VD, VD]] = {

    val ms = new MatchSet(rdfGraph, triplePattern.collect(), session)
    var finalMatchSet = ms.matchCandidateSet
    var tempMatchSet = ms.matchCandidateSet
    var changed = true
    while(changed) {
      tempMatchSet = ms.validateRemoteMatchSet(ms.validateLocalMatchSet(tempMatchSet))
      if(tempMatchSet.count().equals(finalMatchSet.count())){
        changed = false
      }
      finalMatchSet = tempMatchSet
    }

    val matchMap = finalMatchSet.map(_.mapping)
    val matchVerties = finalMatchSet.map(_.vertex).distinct()
    val matchVaribles = finalMatchSet.map(_.variable).distinct()

    var bgpMapping = Array[Map[VD,VD]]()
    ms.tpList.foreach{ tp =>
      val tpMapping = finalMatchSet.filter(_.tp.equals(tp)).map(_.mapping).collect().map(_.filterKeys(_.toString.startsWith("?")))
      if(bgpMapping.isEmpty){
        bgpMapping = tpMapping
      }
      else{
        bgpMapping = arrayOfMapJoin(bgpMapping, tpMapping)
      }
    }
    bgpMapping.distinct
  }

  private def arrayOfMapJoin[VD](a: Array[Map[VD,VD]], b: Array[Map[VD,VD]]): Array[Map[VD,VD]] = {
    var c = Array[Map[VD,VD]]()
    a.foreach(x => b.foreach(y => c = c :+ x.++(y)))
    c
  }
}
