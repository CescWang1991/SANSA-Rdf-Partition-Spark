import net.sansa_stack.rdf.partition.spark.utils.{BaselineHashPartitions, InitialGraph, SemanticHashPartitions}
import org.apache.spark.TaskContext
import org.apache.spark.graphx.{EdgeDirection, PartitionStrategy}
import org.apache.spark.sql.SparkSession

object RDDPartitionTest {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local[4]").appName("Rdf Graph Partitioning").getOrCreate()
    val sc = session.sparkContext
    val path = args(0)
    val graph = InitialGraph.apply(session, path).cache()
    val bhp = BaselineHashPartitions.apply(graph, PartitionStrategy.RandomVertexCut)
    val shp = new SemanticHashPartitions(bhp, 2, sc)
    /*println("Vertices___")
    bhp.vertices.foreachPartition(it=>println(TaskContext.getPartitionId+","+it.length))
    println("___________")
    shp.vertices.foreachPartition(it=>println(TaskContext.getPartitionId+","+it.length))
    println("Edges______")
    bhp.edges.foreachPartition(it=>println(TaskContext.getPartitionId+","+it.length))
    println("___________")
    shp.edges.foreachPartition(it=>println(TaskContext.getPartitionId+","+it.length))*/
    //println(shp.graph.triplets.count)
  }
}