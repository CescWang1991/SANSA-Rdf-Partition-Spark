package net.sansa_stack.rdf.query.graph.jena

import org.apache.jena.query.{QueryExecution, QueryExecutionFactory}

import scala.collection.JavaConversions._
import scala.io.Source

class Construct{

}

object Construct {
  def main(args: Array[String]): Unit = {
    val path = "src/resources/BSBM/query12.txt"
    val queryString = Source.fromFile(path).mkString
    val queryExec = QueryExecutionFactory.create(Source.fromFile(path).mkString)
  }
}
