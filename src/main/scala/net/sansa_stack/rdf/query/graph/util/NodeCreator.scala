package net.sansa_stack.rdf.query.graph.util

import org.apache.jena.graph.{Node, NodeFactory}

object NodeCreator {

  def create(term: String): Node = {
    // return node variable
    if(term.startsWith("?")){
      val newTerm = term.split("\\?")(1)
      NodeFactory.createVariable(newTerm)
    }
    // return node URL
    else if(term.startsWith("<") && term.endsWith(">")){
      val newTerm = term.split("\\<")(1).split("\\>")(0)
      NodeFactory.createURI(newTerm)
    }
    // return node Literals
    else if(term.startsWith("\"")){
      NodeFactory.createLiteral(term)
    }
    else{
      NodeFactory.createBlankNode(term)
    }
  }
}
