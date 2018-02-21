import net.sansa_stack.rdf.partition.spark.strategy.{PathPartitionStrategy, SemanticHashPartitionStrategy}
import net.sansa_stack.rdf.partition.spark.utils.{EndToEndPaths, InitialGraph, TripleGroup, TripleGroupType}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

object SHPTest {
  def main(args: Array[String]): Unit = {
    //val path = "src/main/resources/SilviaClustering_HairStylist_TaxiDriver.nt"
    val path = "src/main/resources/Clustering_sampledata1.nt"
    val session = SparkSession.builder().master("local[4]").appName("Rdf Graph Partitioning").getOrCreate()
    val graph = InitialGraph.apply(session, path).cache()

    /*val shps = new SemanticHashPartitionStrategy(graph,session,2)
      .setNumHop(2).setTripleGroupType(TripleGroupType.s)
    shps.getVertices().foreachPartition(it=>println(TaskContext.getPartitionId()+": "+it.mkString(",")))
    shps.partitionBy().edges.foreachPartition(it=>println(TaskContext.getPartitionId()+": "+it.mkString(",")))*/

    /*val pp = new PathPartitionStrategy(graph, session, 2)
    pp.getVertices().foreachPartition(it=>println(TaskContext.getPartitionId()+": "+it.mkString(",")))
    pp.partitionBy().edges.foreachPartition(it=>println(TaskContext.getPartitionId()+": "+it.mkString(",")))*/

    /*val pathGraph = EndToEndPaths.run(graph)
    val pathAsEdge = pathGraph.collect.map{ case(vid,vlist) =>
      val newPath = vlist.flatMap(path =>
        path.sliding(2).map(pair =>
          graph.edges.filter(e =>
            e.srcId==pair(0)&&e.dstId==pair(1)).first).toList).distinct
      (vid,newPath)
    }
    pathAsEdge.foreach(println(_))*/

    val pps = new PathPartitionStrategy(graph, session, 2)
    pps.getVertices().foreachPartition(it=>println(TaskContext.getPartitionId()+": "+it.mkString(",")))
    pps.partitionBy().edges.foreachPartition(it=>println(TaskContext.getPartitionId()+": "+it.mkString(",")))
  }
}
