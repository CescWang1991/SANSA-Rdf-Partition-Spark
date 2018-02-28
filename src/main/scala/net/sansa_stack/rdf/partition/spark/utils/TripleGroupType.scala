package net.sansa_stack.rdf.partition.spark.utils

object TripleGroupType extends Enumeration{
  type TripleGroupType = Value
  val s, o, so = Value
}