package net.sansa_stack.rdf.query.graph.jena.exprFilter
import org.apache.jena.graph.Node

class ExprBound(variable: Node) extends ExprFilter {

  private val tag = "Filter Bound"
  private var logic = true

  override def evaluate(solution: Map[Node, Node]): Boolean = {
    if(solution.keySet.contains(variable)){
      logic
    }
    else {
      !logic
    }
  }

  override def getTag: String = { tag }

  def setLogic(logic: Boolean): Unit = {
    this.logic = logic
  }
}
