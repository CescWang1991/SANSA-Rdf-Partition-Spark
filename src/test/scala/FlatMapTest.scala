object FlatMapTest {
  def main(args: Array[String]): Unit = {
    val s = Set[(Int,Set[(Int,String)])]((1,Set((3,"c"),(4,"d"),(5,"e"))),(4,Set((1,"a"),(2,"b"),(3,"c"))))
    val node = Set[(Int,String)]((1,"a"),(4,"d"),(5,"e"),(6,"f"))
    val expand = node.flatMap(i=>s.find{case(id,_)=>id==i._1}.map(v=>v._2)).flatten
    println(expand)
    expand.toIterator.foreach(println(_))
  }
}
