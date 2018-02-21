import net.sansa_stack.rdf.partition.spark.evaluation.PartitionStrategyMetrics
import net.sansa_stack.rdf.partition.spark.strategy.{PathPartitionStrategy, SemanticHashPartitionStrategy}
import net.sansa_stack.rdf.partition.spark.utils.{InitialGraph, TripleGroupType}
import org.apache.spark.TaskContext
import org.apache.spark.graphx.{EdgeRDD, Graph, VertexRDD}
import org.apache.spark.sql.SparkSession

object EvaluationTest {
  def main(args: Array[String]): Unit = {
    //val path_1 = "src/main/resources/SilviaClustering_HairStylist_TaxiDriver.nt"
    val path = "src/main/resources/Clustering_sampledata1.nt"
    val session = SparkSession.builder().master("local[4]").appName("Rdf Graph Partitioning").getOrCreate()
    val graph = InitialGraph.apply(session, path).cache()

    /*val pps = new PathPartitionStrategy(graph)
    val psm = new PartitionStrategyMetrics(pps,2)
    println("Duplication: "+psm.duplication())*/
  }
}