import org.apache.spark.SparkConf
object DepthFirstSearch {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\winutils" )
    type Vertex = Int
    type Graph = Map[Vertex, List[Vertex]]
//    val x = Map;
    val g: Graph = Map(1 -> List(2,4,7), 2 -> List(5,6), 3 -> List(), 4 ->
                      List(),5 -> List(7),6 -> List(3),7 -> List())
    def DFS(start: Vertex, g: Graph): List[Vertex] = {
      def DFS0(v: Vertex, visited: List[Vertex]): List[Vertex] = {
        if (visited.contains(v))
          visited
        else {
          val neighbours:List[Vertex] = g(v) filterNot visited.contains
          neighbours.foldLeft(v :: visited)((b,a) => DFS0(a,b))
        }
      }
      DFS0(start, List()).reverse
    }
    val result = DFS(1, g )
    println(result.mkString(","))
  }
}