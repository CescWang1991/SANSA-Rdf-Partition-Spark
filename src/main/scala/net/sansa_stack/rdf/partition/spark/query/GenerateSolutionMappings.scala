package net.sansa_stack.rdf.partition.spark.query

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object GenerateSolutionMappings {

  /*def run[VD: ClassTag, ED: ClassTag](ms: MatchSet[VD,ED]) = {
    var omegaBGP = ArrayBuffer[ArrayBuffer[(Map[VD,VD],Map[VD,VD])]]()
    ms.tpList.foreach{tp =>
      var omegaTP = ArrayBuffer[(Map[VD,VD],Map[VD,VD])]()
      ms.matchVertices.foreach{ v =>
        ms.matchCandidateSet().filter(_._1.equals(v)).foreach{ case(_,matchMap) =>
          if(matchMap._3.equals(tp)){

          }
        }
      }
      omegaBGP = omegaBGP
    }
  }*/
}
