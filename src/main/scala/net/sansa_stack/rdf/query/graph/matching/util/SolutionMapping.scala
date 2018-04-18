package net.sansa_stack.rdf.query.graph.matching.util

import scala.reflect.ClassTag

/**
  * A class represent one solution mapping as a result for SPARQL query.
  *
  * @tparam VD Attribute of the key and value
  * @author Zhe Wang
  */
class SolutionMapping[VD: ClassTag](map: Map[VD, VD]) extends Serializable {

}
