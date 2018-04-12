package net.sansa_stack.rdf.query.graph.matching.util

import org.apache.spark.graphx.EdgeTriplet

import scala.reflect.ClassTag

/**
  * A basic notion in SPARQL where every part is either an RDF term or a variable.
  *
  * @tparam VD the type of the vertex attribute.
  * @tparam ED the type of the edge attribute
  *
  * @author Zhe Wang
  */
class TriplePattern[VD: ClassTag, ED: ClassTag](t:VD, p: ED, o: VD) extends Serializable {

  import TriplePattern._

  val srcAttr: VD = t
  val attr: ED = p
  val dstAttr: VD = o

  /**
    * Get the subject of this query triple
    *
    * @return a tuple (subject attribute, is variable)
    */
  def getSubject: (VD, Boolean) = {
    (srcAttr, isVariable[VD](srcAttr))
  }

  /**
    * Get the object of this query triple
    *
    * @return a tuple (object attribute, is variable)
    */
  def getObject: (VD, Boolean) = {
    (dstAttr, isVariable[VD](dstAttr))
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
    val sub = checkQueryPart[VD]((srcAttr, isVariable[VD](srcAttr)), triplet.srcAttr)
    val pred = checkQueryPart[ED](getPredicate, triplet.attr)
    val obj = checkQueryPart[VD]((dstAttr, isVariable[VD](dstAttr)), triplet.dstAttr)
    sub & pred & obj
  }

  def getVariable: Array[VD] = {
    if(isVariable[VD](srcAttr)){
      if(isVariable[VD](dstAttr)){
        Array(srcAttr, dstAttr)
      }
      else{
        Array(srcAttr)
      }
    }
    else{
      if(isVariable[VD](dstAttr)){
        Array(dstAttr)
      }
      else{
        Array()
      }
    }
  }

  override def toString(): String = (srcAttr, dstAttr, attr).toString()
}

object TriplePattern{

  def apply[VD: ClassTag, ED: ClassTag](t: VD, p: ED, o: VD): TriplePattern[VD,ED] = new TriplePattern(t, p, o)

  /**
    * Check if the given attribute is a variable field or not.
    *
    * @param attr attribute to test with type T
    * @return True if the attribute startsWith "?", else false
    */
  def isVariable[T](attr: T): Boolean = {
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
