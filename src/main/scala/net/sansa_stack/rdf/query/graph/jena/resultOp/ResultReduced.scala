package net.sansa_stack.rdf.query.graph.jena.resultOp

import org.apache.jena.graph.Node

/**
  * Class that execute REDUCED modifier. Support syntax as SELECT REDUCED ?user WHERE ...
  */
class ResultReduced extends ResultOp {

  private val tag = "REDUCED"

  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    var duplicates = input.groupBy(identity).mapValues(_.length).filter{ case(_, count) =>
      count > 1}.keys.toArray
    input.filter(map =>
      if(duplicates.contains(map)){
        duplicates = duplicates.filterNot(_.equals(map))
        false
      }
      else{ true })
  }

  override def getTag: String = { tag }
}
