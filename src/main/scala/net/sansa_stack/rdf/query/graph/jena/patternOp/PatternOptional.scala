package net.sansa_stack.rdf.query.graph.jena.patternOp

import net.sansa_stack.rdf.query.graph.jena.resultOp.{ResultFilter, ResultOp}
import net.sansa_stack.rdf.query.graph.jena.{BasicGraphPattern, ExprParser, Ops, SparqlParser}
import net.sansa_stack.rdf.query.graph.matching.GenerateSolutionMappings
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.sparql.algebra.op.{OpBGP, OpLeftJoin}
import org.apache.jena.sparql.expr.ExprList
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Class that execute SPARQL OPTIONAL operation
  * @param bgp Basic Pattern for optional
  */
class PatternOptional(bgp: Iterator[Triple], exprs: ExprList) extends PatternOp {

  private val tag = "OPTIONAL"
  private val ops = new mutable.Queue[ResultFilter]()

  override def execute(input: Array[Map[Node, Node]],
                       graph: Graph[Node, Node],
                       session: SparkSession): Array[Map[Node, Node]] = {
    var optional = GenerateSolutionMappings.run[Node, Node](graph,
      BasicGraphPattern(bgp, session.sparkContext).triplePatterns,
      session)
    if(!(exprs==null)){
      exprs.foreach(expr => ops.enqueue(new ResultFilter(expr)))
      ops.foreach(op => optional = op.asInstanceOf[ResultOp].execute(optional))
    }

    leftJoin(input, optional)
  }

  override def getTag: String = { tag }

  private def leftJoin(a: Array[Map[Node,Node]], b: Array[Map[Node,Node]]): Array[Map[Node,Node]] = {
    var c = Array[Map[Node,Node]]()
    if(a.head.keySet.intersect(b.head.keySet).isEmpty){   //two arrays have no common keys
      a.foreach(x => b.foreach(y => c = c :+ x.++(y)))
      c
    } else if(a.head.keySet.intersect(b.head.keySet).size == 1){  //two arrays has one common keys
      val intVar = a.head.keySet.intersect(b.head.keySet).head
      var d = a
      a.foreach(x =>
        b.foreach(y =>
          if(x.get(intVar).equals(y.get(intVar))){    //adding bindings to one or more solutions
            c = c :+ x.++(y)
            d = d.filterNot(_.equals(x))
          }))
      c ++ d                                          // adding unchanged solutions
    } else {  //two arrays has two common keys
      val intVar = a.head.keySet.intersect(b.head.keySet)
      a.foreach(x =>
        b.foreach(y =>
          if(x.get(intVar.head).equals(y.get(intVar.head)) && x.get(intVar.tail.head).equals(y.get(intVar.tail.head))){
            c = c :+ x.++(y)
          }))
      c
    }
  }
}
