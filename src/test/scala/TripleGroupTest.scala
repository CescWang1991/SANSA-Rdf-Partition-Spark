import net.sansa_stack.rdf.partition.spark.utils._
import org.apache.spark.TaskContext
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.sql.SparkSession

object TripleGroupTest {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local[4]").appName("Rdf Graph Partitioning").getOrCreate()
    val sc = session.sparkContext
    val path = args(0)
    val graph = InitialGraph.apply(session, path).cache()
    val bhp = BaselineHashPartitions.apply(graph, PartitionStrategy.RandomVertexCut)
    val shp = new SemanticHashPartitions(bhp, 3, sc)
    val stg = new TripleGroup(bhp,TripleGroupType.s)
    val verticesWithEdgeGroupSet = stg.edgesGroupSet.toLocalIterator.toArray.map{ case(id,_) => id}
    val edgesGroupSet = stg.edgesGroupSet.toLocalIterator.toArray

    shp.vertices.foreachPartition { it =>
      val anchorVertices = it.filter { case (id, _) => verticesWithEdgeGroupSet.contains(id) }.toArray
      println("Partition"+TaskContext.getPartitionId+" has "+anchorVertices.length+" vertices with stg")
      println("Partition"+TaskContext.getPartitionId+" has "+anchorVertices.map(vertex=>edgesGroupSet.find{case(anchorVertexId,_) =>
        anchorVertexId == vertex._1}.get._2.length).reduce((x,y)=>x+y)+" edges")
    }
  }
}
