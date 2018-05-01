package net.sansa_stack.rdf.query.graph.jena.resultOp

import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.op.OpOrder

import scala.collection.JavaConversions._

/**
  * Class that execute SPARQL ORDERBY operation. Currently support for ordering by at most four variables in the same time.
  * @param op Order operator
  */
class ResultOrder(op: OpOrder) extends ResultOp {

  private val tag = "ORDER BY"

  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    val vars = op.getConditions.toList.map(_.expression.asVar().asNode())
    val dirs = op.getConditions.toList.map(_.direction)
    vars.length match {
      case 1 => input.sortBy(m =>
        m(vars.head).toString)(order1(dirs.head))
      case 2 => input.sortBy(m =>
        (m(vars.head).toString, m(vars(1)).toString))(order2(dirs.head, dirs(1)))
      case 3 => input.sortBy(m =>
        (m(vars.head).toString, m(vars(1)).toString, m(vars(2)).toString))(order3(dirs.head, dirs(1), dirs(2)))
      case 4 => input.sortBy(m =>
        (m(vars.head).toString, m(vars(1)).toString, m(vars(2)).toString, m(vars(3)).toString)
      )(order4(dirs.head, dirs(1), dirs(2), dirs(3)))
      case _ => println("No support order by more than 4 variables")
        input
    }
  }

  def test(input: Array[Map[Node, Node]]): Unit = {
    val vars = op.getConditions.toList.map(_.expression.asVar().asNode())
    val dirs = op.getConditions.toList.map(_.direction)
    vars.length match {
      case 2 => println(vars.length)
      case 3 => println("gg")
    }
    input.sortBy( m =>
      (m(vars.head).toString, m(vars(1)).toString))(order2(dirs.head, dirs(1))).foreach(println(_))
  }

  override def getTag: String = { tag }

  private def order1(a: Int): Ordering[String] = {
    orderDec(a)
  }

  private def order2(a: Int, b: Int): Ordering[(String, String)] = {
    Ordering.Tuple2(orderDec(a), orderDec(b))
  }

  private def order3(a: Int, b: Int, c: Int): Ordering[(String, String, String)] = {
    Ordering.Tuple3(orderDec(a), orderDec(b), orderDec(c))
  }

  private def order4(a: Int, b: Int, c: Int, d: Int): Ordering[(String, String, String, String)] = {
    Ordering.Tuple4(orderDec(a), orderDec(b), orderDec(c), orderDec(d))
  }

  private def orderDec(direction: Int): Ordering[String] = {
    direction match {
      case -2 => Ordering.String
      case -1 => Ordering.String.reverse
    }
  }
}
