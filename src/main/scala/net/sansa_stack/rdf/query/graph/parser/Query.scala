package net.sansa_stack.rdf.query.graph.parser

import net.sansa_stack.rdf.query.graph.sparql.TriplePattern
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Class that parser the sparql query expression
  *
  * @param sparkContext spark context
  * @param path path to file that contains sparql query
  *
  * @author Zhe Wang
  */
class Query(sparkContext: SparkContext, path: String) extends Serializable {
  private val file = Source.fromFile(path)
  private var declarations = ArrayBuffer.empty[String]
  private var varFields = ArrayBuffer.empty[String]
  private var patterns = ArrayBuffer.empty[String]
  //private var ops = ArrayBuffer.empty[String]
  private val logger = LoggerFactory.getLogger(classOf[Query])

  for (line <- file.getLines()){
    if(line.contains("PREFIX")){
      declarations += line
    }
    else if(line.contains("SELECT") && line.contains("WHERE")){
      line.split("SELECT")
    }
    else if(line.contains("SELECT")){
      val parts = line.split(" +").map(_.trim)
      if(parts(0).equals("SELECT")){
        parts.drop(1).foreach(variable => varFields += variable)
      }
      else{
        logger.info("The expression of Sparql query is not correct, please write like SELECT ?title ?price")
      }
    }
    else if(line.contains("WHERE")){
      val parts = line.split("WHERE").map(_.trim)
      // if WHERE and triple are in the same line
      scala.util.control.Exception.ignoring(classOf[ArrayIndexOutOfBoundsException]) {
        val triple = parts(1).split("\\{")(1).trim
        if(triple.contains("}")){
          patterns += triple.split("\\}")(0).trim
        }
        else{ patterns += triple }
      }
    }
    else{
      if(line.contains("{")){
        if(line.contains("}")){
          patterns += line.split("\\{")(1).trim.split("\\}")(0)
        }
        else{
          patterns += line.split("\\{")(1).trim
        }
      }
      else{
        if(line.contains("}")){
          patterns += line.split("\\}")(0).trim
        }
        else{
          patterns += line.trim
        }
      }
    }
  }

  val triplePatterns: Array[String] = {
    val prefix = new PrefixUtil
    prefix.extendPrefixesMap(declarations.toArray)
    patterns.toArray.map{ line =>
      val parts = line.split(" +").map(_.trim)
      //new TriplePattern(prefix.replacePrefix(parts(0)), prefix.replacePrefix(parts(1)), prefix.replacePrefix(parts(2)))
      prefix.replacePrefix(parts(0))+" "+prefix.replacePrefix(parts(1))+" "+prefix.replacePrefix(parts(2))
    }
  }
}
