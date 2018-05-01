package net.sansa_stack.rdf.query.graph.matching.util

import net.sansa_stack.rdf.query.graph.jena.patternOp.PatternOp
import net.sansa_stack.rdf.query.graph.jena.resultOp.{ResultOp, ResultProject}
import net.sansa_stack.rdf.query.graph.jena.{BasicGraphPattern, SparqlParser}
import net.sansa_stack.rdf.query.graph.matching.GenerateSolutionMappings
import net.sansa_stack.rdf.spark.graph.LoadGraph
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * Object to create result of graph query from target rdf graph.
  *
  * @author Zhe Wang
  */
object ResultFactory {
  /**
    * Create a result rdd from the given mapping.
    *
    * @param mapping  The given solution mapping
    * @param session  Spark session
    * @tparam VD  Attribute of variables and rdf terms
    */
  def create[VD: ClassTag](mapping: Array[Map[VD, VD]], session: SparkSession): RDD[Result[VD]] = {
    val result = session.sparkContext.parallelize(mapping.map{ array =>
      new Result[VD].addAllMapping(array)
    })
    result
  }

  def convertToDataFrame[VD: ClassTag](results: RDD[Result[VD]], session: SparkSession): Unit = {
    //session.createDataFrame()
  }

  def main(args: Array[String]): Unit = {
    val ntPath = "src/resources/Rdf/Clustering_sampledata.nt"
    val spPath = "src/resources/Sparql/SelectProject.txt"
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val sp = new SparqlParser(spPath)
    val graph = LoadGraph.apply (NTripleReader.load (session, ntPath))
    val bgp = BasicGraphPattern(sp.getElementTriples.toIterator, session.sparkContext)
    val solutionMapping = GenerateSolutionMappings.run[Node, Node](graph, bgp.triplePatterns, session)
    val results = ResultFactory.create[Node](solutionMapping, session)//.foreach(println(_))
    sp.getOps.head.asInstanceOf[ResultProject].test(results, session).foreach(println(_))
  }
}
