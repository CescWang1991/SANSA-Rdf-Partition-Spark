import net.sansa_stack.rdf.query.graph.sparql.SparqlParser
import net.sansa_stack.rdf.query.graph.util.TriplePattern
import org.apache.jena.graph.Node
import org.apache.jena.query.QueryFactory

object QueryReader {
  def main(args: Array[String]): Unit = {
    val path = "src/resources/SparqlQuery.txt"
    val op = new SparqlParser(path)
    op.OpVisitorWalker()
    //val tp = op.getElementTriples.map(t => TriplePattern[Node, Node](t.getSubject, t.getPredicate, t.getObject))

  }
}
