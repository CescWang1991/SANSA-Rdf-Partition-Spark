package net.sansa_stack.rdf.partition.spark.query

import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.rdd.RDD

class BasicGraphPattern[VD,ED] extends GraphImpl[VD,ED] with Serializable {
  var triplePattern: RDD[TriplePattern[VD,ED]] = _
}
