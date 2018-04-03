package net.sansa_stack.rdf.query.graph.parser

import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import net.sansa_stack.rdf.spark.graph.LoadGraph


/**
  * Class that parser the sparql query expression
  *
  * @param sparkContext spark context
  * @param path path to file that contains sparql query
  *
  * @author Zhe Wang
  */
class BasicGraphPattern(sparkContext: SparkContext, path: String) extends Serializable {

  val query = new QueryCreator(path)

  val triples: RDD[Triple] = {
    val prefix = new PrefixUtil
    prefix.extendPrefixesMap(query.getDeclaration.toArray)
    sparkContext.parallelize(query.getPattern.toArray.map{ line =>
      val parts = line.split(" +").map(_.trim)
      val s = NodeCreator.create(prefix.replacePrefix(parts(0)))
      val p = NodeCreator.create(prefix.replacePrefix(parts(1)))
      val o = NodeCreator.create(prefix.replacePrefix(parts(2)))
      new Triple(s, p ,o)
    })
  }

  lazy val graph: Graph[Node, Node] = {
    LoadGraph.apply(triples)
  }

  lazy val triplePattern: RDD[TriplePattern[Node, Node]] = {
    triples.map{ t =>
      new TriplePattern[Node, Node](t.getSubject, t.getPredicate, t.getObject)
    }
  }
}
