package net.sansa_stack.rdf.query.graph.jena.expression

import net.sansa_stack.rdf.query.graph.matching.util.SolutionMapping
import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD

trait ExprFilter extends Serializable {

  def evaluate(solution: Map[Node, Node]): Boolean

  def getTag: String

}
