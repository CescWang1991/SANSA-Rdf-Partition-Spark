package net.sansa_stack.rdf.query.graph.parser

import org.slf4j.LoggerFactory

/**
  * Class that contains methods and attributes to handle prefixes.
  *
  * @author Zhe Wang
  */
class PrefixUtil extends Serializable {

  /**
    * Mapping of popular prefixes to its IRIs that are supported by the parser.
    */
  private var prefixes: Map[String, String] = Map[String, String]("foaf:" -> "<http://xmlns.com/foaf/0.1/",
    "dc:" -> "<http://purl.org/dc/elements/1.1/",
    "rdf:" -> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "bench:" -> "<http://localhost/vocabulary/bench/",
    "ub:" -> "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#",
    "bsbm:" -> "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/",
    "bsbm-inst:" -> "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/",
    "bsbm-export:" -> "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/export/",
    "xsd:" -> "<http://www.w3.org/2001/XMLSchema#",
    "gr:" -> "<http://purl.org/goodrelations/",
    "gn:" -> "<http://www.geonames.org/ontology#",
    "mo:" -> "<http://purl.org/ontology/mo/",
    "og:" -> "<http://ogp.me/ns#",
    "rev:" -> "<http://purl.org/stuff/rev#",
    "sorg:" -> "<http://schema.org/",
    "wsdbm:" -> "<http://db.uwaterloo.ca/~galuc/wsdbm/",
    "dcterms:" -> "<http://purl.org/dc/terms/",
    "dctype:" -> "<http://purl.org/dc/dcmitype/",
    "rdfs:" -> "<http://www.w3.org/2000/01/rdf-schema#",
    "swrc:" -> "<http://swrc.ontoware.org/ontology#",
    "rev:" -> "<http://purl.org/stuff/rev#",
    "rss:" -> "<http://purl.org/rss/1.0/",
    "owl:" -> "<http://www.w3.org/2002/07/owl#",
    "person:" -> "<http://localhost/persons/",
    "ex:" -> "<http://example.org/"
  )

  def extendPrefixesMap(declaration: Array[String]): Unit ={
    declaration.foreach { line =>
      val parts = line.split("PREFIX").map(_.trim)
      val logger = LoggerFactory.getLogger(classOf[PrefixUtil])
      try {
        val key = parts(1).split("<")(0).trim
        val value = ("<" + parts(1).split("<")(1).trim).split(">")(0)
        prefixes += (key -> value)
      }
      catch {
        case e: ArrayIndexOutOfBoundsException =>
          logger.info("Prefix is not declared formally, try to write it like PREFIX foaf:   <http://xmlns.com/foaf/0.1/>")
      }
    }
  }

  /**
    * Collapses predefined prefix to IRIs if the RDF term can be presented as IRI.
    * If input has no prefix, just return the input.
    *
    * @param input an input RDF term.
    * @return IRI if the the input has prefix.
    */
  def replacePrefix(input: String): String = {
    //check if the RDF term has prefix
    if(input.contains(":")) {
      val prefix = input.split(":")(0)+":"
      val suffix = input.replaceFirst(prefix,"")
      if(prefixes.keys.toList.contains(prefix)){
        prefixes(prefix)+suffix+">"
      }
      else{ input }
    }
    else {
      input
    }
  }

  def getPrefixes: Map[String, String] = {
    prefixes
  }
}