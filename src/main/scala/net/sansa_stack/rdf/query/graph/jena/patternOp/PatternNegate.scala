package net.sansa_stack.rdf.query.graph.jena.patternOp
import net.sansa_stack.rdf.query.graph.jena.BasicGraphPattern
import net.sansa_stack.rdf.query.graph.matching.GenerateSolutionMappings
import org.apache.jena.graph.Node
import org.apache.jena.sparql.core.BasicPattern
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.util.control.Breaks._

/**
  * Class that execute SPARQL MINUS and FILTER NOT operations
  * @param bgp Basic Pattern for negation
  */
class PatternNegate(bgp: BasicPattern) extends PatternOp {

  private val tag = "FILTER NOT EXISTS / MINUS"

  override def execute(input: Array[Map[Node, Node]],
                       graph: Graph[Node, Node],
                       session: SparkSession): Array[Map[Node, Node]] = {
    val negation = GenerateSolutionMappings.run[Node, Node](graph,
      BasicGraphPattern(bgp.toIterator, session.sparkContext).triplePatterns,
      session)
    val intVar = input.head.keySet.intersect(negation.head.keySet).toList
    input.filterNot{ i =>
      var neg = false
      breakable {
        negation.foreach { n =>
          intVar.length match {
            case 0 => neg = false
            case _ => if (mapEquals(i, n, intVar)) { neg = true
              break()
            }
          }
        }
      }
      neg
    }
  }

  override def getTag: String = { tag }

  private def mapEquals(a: Map[Node, Node], b: Map[Node, Node], intVar: List[Node]): Boolean = {
    var eq = true
    for( v <- intVar){
      eq = eq && a(v).equals(b(v))
    }
    eq
  }
}
