package net.sansa_stack.rdf.partition.spark.query

import org.apache.spark.graphx.{EdgeTriplet, VertexId}

/**
  * A basic notion in SPARQL where every part is either an RDF term or a variable.
  *
  * @tparam VD the type of the vertex attribute.
  * @tparam ED the type of the edge attribute
  *
  * @author Zhe Wang
  */
class TriplePattern[VD,ED] extends EdgeTriplet[VD,ED] with Serializable {

  import TriplePattern._

  /**
    * Get the subject of this query triple
    *
    * @return a tuple (subject attribute, is variable)
    */
  def getSubject: (VertexId, VD, Boolean) = {
    (srcId, srcAttr, isVariable[VD](srcAttr))
  }

  /**
    * Get the object of this query triple
    *
    * @return a tuple (object attribute, is variable)
    */
  def getObject: (VertexId, VD, Boolean) = {
    (dstId, dstAttr, isVariable[VD](dstAttr))
  }

  /**
    * Get the predicate of this query triple
    *
    * @return a tuple (predicate attribute, is variable)
    */
  def getPredicate: (ED, Boolean) = {
    (attr, isVariable[ED](attr))
  }

  /**
    * Check if a graph triplet fulfills this sparql triple pattern.
    *
    * @param triplet a graph triplet
    * @return true iff subject, predicate and object fulfill this triple
    */
  def isFulfilledByTriplet(triplet: EdgeTriplet[VD,ED]): Boolean = {
    val sub = checkQueryPart[VD]((getSubject._2, getSubject._3), triplet.srcAttr)
    val pred = checkQueryPart[ED](getPredicate, triplet.attr)
    val obj = checkQueryPart[VD]((getObject._2, getObject._3), triplet.dstAttr)
    sub & pred & obj
  }

  /*override def toString: String = (
    (srcId, srcAttr, isVariable[VD](srcAttr)),
    (dstId, dstAttr, isVariable[VD](dstAttr)),
    (attr, isVariable[ED](attr))).toString()*/
  override def toString(): String = (srcAttr, attr, dstAttr).toString()
}

object TriplePattern {

  def apply[VD,ED] = new TriplePattern[VD,ED]

  /**
    * Check if the given attribute is a variable field or not.
    *
    * @param attr attribute to test with type T
    * @return True if the attribute startsWith "?", else false
    */
  private def isVariable[T](attr: T): Boolean = {
    if (attr.toString.startsWith("?")) true
    else false
  }

  /**
    * Checks a part of a triplet against the part of the Sparql triple pattern
    *
    * @param queryPart a part of triple pattern
    * @param tripletPart a part of target triplet to be fulfilled
    *
    * @return True if the attribute fulfill the Sparql triple pattern
    */
  private def checkQueryPart[T](queryPart: (T, Boolean), tripletPart: T): Boolean = {
    if(queryPart._2){
      //If the part is a variable, it fulfill every field
      true
    }
    else {
      //Check whether the attribute is equal
      queryPart._1.equals(tripletPart)
    }
  }
}
