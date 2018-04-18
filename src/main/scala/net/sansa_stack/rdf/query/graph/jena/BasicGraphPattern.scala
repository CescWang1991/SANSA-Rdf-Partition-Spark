package net.sansa_stack.rdf.query.graph.jena

import net.sansa_stack.rdf.query.graph.matching.util.TriplePattern
import net.sansa_stack.rdf.spark.graph.LoadGraph
import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD


/**
  * Class that generate GraphX graph for input triples
  *
  * @param sc spark context
  * @param triples input triples to generate graph patterns
  *
  * @author Zhe Wang
  */
class BasicGraphPattern(triples: Iterator[Triple], sc: SparkContext) extends Serializable {

  val tripleRDD: RDD[Triple] = sc.parallelize(triples.toList)

  lazy val graph: Graph[Node, Node] = {
    LoadGraph(tripleRDD)
  }

  lazy val triplePatterns: RDD[TriplePattern[Node, Node]] = {
    tripleRDD.map( t => TriplePattern(t.getSubject, t.getPredicate, t.getObject))
  }

  lazy val numTriple: Long = tripleRDD.count()
}

object BasicGraphPattern{
  def apply(triples: Iterator[Triple], sc: SparkContext): BasicGraphPattern = new BasicGraphPattern(triples, sc)
}
