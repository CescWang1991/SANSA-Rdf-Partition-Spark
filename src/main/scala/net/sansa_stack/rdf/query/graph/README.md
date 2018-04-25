# Sparql To GraphX

### Introduction<a name="introduction"></a>
This package provide a interface to convert [Sparql 1.1 Query Language](https://www.w3.org/TR/sparql11-query/) to GraphX, which aim to use Sparql queries to generate [a property graph](https://spark.apache.org/docs/latest/graphx-programming-guide.html#the-property-graph) which is a class of package [org.apache.spark.graphx](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.package). In this case, we use Apache Jena query engine [ARQ](https://jena.apache.org/documentation/query/) to parser [SPARQL RDF Query language](https://www.w3.org/TR/sparql11-query/), convert basic graph pattern to graph in which the type of vertices and edges are [Node](https://jena.apache.org/documentation/javadoc/jena/org/apache/jena/graph/Node.html). The graph with variables can match target rdf graph partitioned by [graph partition algorithm](../../partition/spark/algo) in parallel and generate solution mappings.

### Graph Patterns
SPARQL is based around graph pattern matching.

#### Example: [BGP Matching](https://www.w3.org/TR/sparql11-query/#GraphPattern)<a name="BGP"></a>
Users write a SPARQL query to be an input, and the Jena query engine will tranform the query as an instance of op. In this case, we write query in [SPARQL file](https://github.com/CescWang1991/SANSA-Rdf-Partition-Spark/blob/develop/src/resources/Sparql/QueryFilter.txt), to match the target [RDF file](https://github.com/CescWang1991/SANSA-Rdf-Partition-Spark/blob/develop/src/resources/Clustering_sampledata.nt).
```sparql
PREFIX tw: <http://twitter/>
SELECT ?user ?follower
WHERE {
    ?user tw:follows tw:user1 .
    tw:user1 tw:follows ?follower .
}
```
We exact the basic graph pattern from the SPARQL file and generate Graph[Node, Node], in which has three vertices RDD((0, "?user"), (1, "<http://twitter/user1>"), (2, "?follower")) and two edges RDD((0, 1, <http://twitter/follows>), (1, 2, <http://twitter/follows>)). The steps to generate basic graph pattern is like:
```scala
val session = SparkSession.builder().master("local[*]").getOrCreate()       // Initialize spark session
val sarqlParser = new SparqlParser(path)                                    // Initialize sparql parser with the path to sparql file 
sp.OpVisitorWalker()                                                        // Walk the query
val bgp = BasicGraphPattern(sp.getElementTriples, session.sparkContext)     // Get basic graph pattern and generate a graph
```
Class [SparqlParser](jena/SparqlParser.scala) will walk the SPARQL query and from which we get a set of triple patterns.

Then we apply an algorithm introduced by paper: [Graph-Parallel Querying of RDF with GraphX](http://www2.informatik.uni-freiburg.de/~schaetzl/papers/S2X_Big-O(Q)_2015.pdf) to generate solution mapping which is the query result. 
```scala
val solutionMapping = GenerateSolutionMappings.run[Node, Node](graph, bgp.triplePatterns, session)
```
In this example, we get:
```scala
Map(?user -> "<http://twitter/user0>", ?follower -> "<http://twitter/user2>")
Map(?user -> "<http://twitter/user0>", ?follower -> "<http://twitter/user3>")
Map(?user -> "<http://twitter/user0>", ?follower -> "<http://twitter/user6>")
```

### Including Optional Values

#### Example: [Optional Pattern Matching](https://www.w3.org/TR/sparql11-query/#OptionalMatching)
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?name ?age
WHERE {
    ?user foaf:age ?age .
    OPTIONAL { ?user foaf:name ?name . }
}
```

#### Example: [Constraints in Optional Pattern Matching](https://www.w3.org/TR/sparql11-query/#OptionalAndConstraints)
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX tw: <http://twitter/>
SELECT DISTINCT ?user ?age
WHERE {
    ?user tw:follows ?follower .
    ?follower foaf:name ?name
    FILTER regex(?name, "Di")
    OPTIONAL { ?user foaf:age ?age . FILTER (?age>25) }
}
```

#### Example: [Multiple Optional Graph Patterns](https://www.w3.org/TR/sparql11-query/#MultipleOptionals)
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX tw: <http://twitter/>
SELECT ?name ?age
WHERE {
    ?user tw:follows tw:user7
    OPTIONAL { ?user foaf:age ?age . }
    OPTIONAL { ?user foaf:name ?name . }
}
```

### Matching Alternatives

#### Example: [UNION](https://www.w3.org/TR/sparql11-query/#alternatives)
```sparql
PREFIX tw: <http://twitter/>
SELECT ?user
WHERE {
    { ?user tw:follows tw:user7 . }
    UNION { ?user tw:follows tw:user2 . }
}
```

### Negation
Execute operations of expressions by the keyword FILTER.

#### Example: Filter Regex<a name="Regex"></a>
```sparql
PREFIX tw: <http://twitter/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?user ?follower
WHERE {
    ?user foaf:name ?name .
    FILTER regex(?name, "Ali")
}
```
Note: Currently only support FILTER regex expression without flags argument.

#### Example: Filter Compare<a name="Compare"></a>
```sparql
PREFIX tw: <http://twitter/>
PREFIX ex: <http://example.org/>
SELECT ?user ?age
WHERE {
    ?user tw:follows tw:user7 .
    ?user ex:age ?age .
    FILTER (?age <= 12)
}
```
Note: Support operators include Equals(==), Not Equals(!=), GreaterThanOrEqual(>=), GreaterThan(>), LessThanOrEqual(<=), LessThan(<).

#### Example: Filtering Pattern<a name="Pattern"></a>
```sparql
PREFIX tw: <http://twitter/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?user
WHERE {
    ?user tw:follows tw:user7 .
    FILTER EXISTS { ?user foaf:name ?name }
}
```
AND
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX tw: <http://twitter/>
SELECT ?name WHERE {
    ?user foaf:name ?name
    FILTER NOT EXISTS {
        ?user  tw:follows ?follower .
        ?follower foaf:name "Diana" .
    }
}
```

#### Example: [Filter Bound](https://www.w3.org/TR/sparql11-query/#func-bound)
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?user ?age
WHERE {
    ?user foaf:age ?age .
    OPTIONAL { ?user foaf:name ?name . }
    FILTER (bound(?name))
}
```

#### Example: [MINUS](https://www.w3.org/TR/sparql11-query/#neg-minus)
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX tw: <http://twitter/>
SELECT ?name WHERE {
    ?user foaf:name ?name
    MINUS {
        ?user  tw:follows ?follower .
        ?follower foaf:name "Diana" .
    }
}
```

### [Property Paths](https://www.w3.org/TR/sparql11-query/#propertypaths)

### Assignment

#### Example: [BIND](https://www.w3.org/TR/sparql11-query/#bind)

#### Example: [VALUES](https://www.w3.org/TR/sparql11-query/#inline-data)

### Aggregates

#### Example: [GROUP BY](https://www.w3.org/TR/sparql11-query/#groupby)
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX tw: <http://twitter/>
SELECT DISTINCT ?user (AVG(?age) as ?ages) (MAX(?age) as ?max) (MIN(?age) as ?min)
WHERE {
    ?user tw:follows ?follower .
    ?follower foaf:age ?age .
} GROUP BY ?user
```
Note: The example is produced by grouping solutions according to the GROUP BY expression. Currently support group by 
simple variables, and support aggregate operation as COUNT, SUM, MIN, MAX, AVG.

#### Example: [HAVING](https://www.w3.org/TR/sparql11-query/#having)
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX tw: <http://twitter/>
SELECT DISTINCT ?user (AVG(?age) as ?ages)
WHERE {
    ?user tw:follows ?follower .
    ?follower foaf:age ?age .
} GROUP BY ?user
HAVING (?ages>15)
```

### Solution Sequences and Modifiers
Sequence modifiers are applied to operate on unordered solutions generated by BGP match.

#### Example: [Order By](https://www.w3.org/TR/sparql11-query/#modOrderBy)<a name="OrderBy"></a>
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX tw: <http://twitter/>
SELECT DISTINCT ?user ?age
WHERE {
    ?user tw:follows ?follower .
    ?user foaf:age ?age .
} ORDER BY ?age
```

#### Example: [Projection](https://www.w3.org/TR/sparql11-query/#modProjection)<a name="Projection"></a>
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?name
WHERE {
    ?user foaf:name ?name .
}
```
Note: Support using * to select all variables and select required variables.

#### Example: [DISTINCT](https://www.w3.org/TR/sparql11-query/#modDuplicates)<a name="Distinct"></a>
```sparql
PREFIX tw: <http://twitter/>
SELECT DISTINCT ?user
WHERE {
    ?user tw:follows ?follower .
}
```

#### Example: [REDUCED](https://www.w3.org/TR/sparql11-query/#modDuplicates)<a name="Reduced"></a>
```sparql
PREFIX tw: <http://twitter/>
SELECT REDUCED ?user
WHERE {
    ?user tw:follows ?follower .
}
```

#### Example: [SLICE](https://www.w3.org/TR/sparql11-query/#modOffset)<a name="Offset"></a>
```sparql
PREFIX tw: <http://twitter/>
SELECT DISTINCT ?user
WHERE {
    ?user tw:follows ?follower .
} LIMIT 3
OFFSET 2
```
