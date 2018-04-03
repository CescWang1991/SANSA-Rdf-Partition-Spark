import org.apache.jena.query.QueryFactory

object QueryReader {
  def main(args: Array[String]): Unit = {
    val query = QueryFactory.read("src/resources/SparqlQuery.txt")
    //println(query.getResultVars.toArray().mkString("\n"))
    println(query.getProjectVars.toArray().mkString("\n"))
  }
}
