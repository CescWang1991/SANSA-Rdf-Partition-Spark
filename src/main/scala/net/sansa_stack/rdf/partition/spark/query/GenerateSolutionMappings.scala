package net.sansa_stack.rdf.partition.spark.query

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object GenerateSolutionMappings {

  def run[VD: ClassTag, ED: ClassTag](ms: MatchSet[VD,ED]): Array[Map[VD,VD]] = {
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
