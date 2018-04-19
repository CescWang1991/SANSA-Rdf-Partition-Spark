package net.sansa_stack.rdf.query.graph.jena.graphOp

import com.intel.analytics.bigdl.nn.quantized.Desc
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.op.OpOrder

import scala.collection.JavaConversions._

class GraphOrder(op: OpOrder) extends GraphOp {

  private val tag = "ORDER BY"

  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    var intermediate = input
    op.getConditions.toList.foreach{ sort =>
      val variable = sort.getExpression.asVar().asNode()
      val ordering = sort.direction
      if(ordering == -2){    // ASC()
        intermediate = intermediate.sortWith(_(variable).toString < _(variable).toString)
      } else {    //DESC()
        intermediate = intermediate.sortWith(_(variable).toString > _(variable).toString)
      }
    }
    val output = intermediate
    output
  }

  def test(input: Array[Map[Node, Node]]): Unit = {
    var intermediate = input
    op.getConditions.toList.foreach{ sort =>
      val variable = sort.getExpression.asVar().asNode()
      val ordering = sort.direction
      /*if(ordering == -2){    // ASC()
        //intermediate.sortWith(_(variable).toString < _(variable).toString).foreach(println(_))
      } else {    //DESC()
        //intermediate.sortWith(_(variable).toString > _(variable).toString).foreach(println(_))
      }*/
    }
  }

  override def getTag: String = { tag }
}
