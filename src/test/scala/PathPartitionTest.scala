import net.sansa_stack.rdf.partition.spark.algo.PathPartition
import net.sansa_stack.rdf.partition.spark.utils.InitialGraph
import org.apache.jena.graph.Node
import org.apache.spark.TaskContext
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

object PathPartitionTest {
  def main(args: Array[String]): Unit = {
    val path = "src/main/resources/AnomalyDetection/ClusteringOntype.nt"
    val session = SparkSession.builder().master("local[4]").appName("Rdf Graph Partitioning").getOrCreate()
    val graph = InitialGraph.apply(session, path).cache()

    val pps = new PathPartition(graph, session, 3)
    pps.partitionBy().triplets.foreachPartition(it=>println("P "+TaskContext.getPartitionId()+": "+it.length))
  }
}
