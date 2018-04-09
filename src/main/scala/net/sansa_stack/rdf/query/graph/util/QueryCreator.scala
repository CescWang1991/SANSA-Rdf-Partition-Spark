package net.sansa_stack.rdf.query.graph.util

import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer
import scala.io.Source


class QueryCreator(path: String) extends Serializable {

  private var declarations = ArrayBuffer.empty[String]
  private var varFields = ArrayBuffer.empty[String]
  private var patterns = ArrayBuffer.empty[String]
  private var ops = ArrayBuffer.empty[String]
  private val logger = LoggerFactory.getLogger(classOf[BasicGraphPattern])

  def getDeclaration: ArrayBuffer[String] = {
    val file = Source.fromFile(path)
    for (line <- file.getLines()){
      if(lineIdentity(line).equals("Prefix Declaration")){
        declarations += line
      }
    }
    file.close()
    declarations
  }

  def getVarField: ArrayBuffer[String] = {
    val file = Source.fromFile(path)
    for (line <- file.getLines()){
      if(lineIdentity(line).equals("Variable Field")){
        val parts = line.split(" +").map(_.trim)
        if(parts(0).equals("SELECT")){
          parts.drop(1).foreach(variable => varFields += variable)
        }
        else{
          logger.info("The expression of sparql query is not correct, please write like SELECT ?title ?price")
        }
      }
    }
    file.close()
    varFields
  }

  def getPattern: ArrayBuffer[String] = {
    val file = Source.fromFile(path)
    for (line <- file.getLines()){
      if(lineIdentity(line).equals("Triple Pattern")){
        if(line.contains("WHERE")){
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
    }
    file.close()
    patterns
  }

  private def lineIdentity(line: String): String = {
    if(line.contains("PREFIX")){ "Prefix Declaration" }
    else if(line.contains("SELECT")){ "Variable Field" }
    else if(line.contains("WHERE")) { "Triple Pattern" }
    else if(line.contains("FILTER")) { "Filter Sentence" }
    else{ "Triple Pattern" }
  }
}
