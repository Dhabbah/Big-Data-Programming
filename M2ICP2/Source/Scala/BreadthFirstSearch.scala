import org.apache.spark.SparkConf

object BreadthFirstSearch {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\winutils" )
    type Vertex = Int
    type Graph = Map[Vertex, List[Vertex]]
    val g: Graph = Map(1 -> List(2,4,7), 2 -> List(5,6), 3 -> List(), 4 -> List(),5 ->
                        List(7),6 -> List(3),7 -> List())
    def BFS(start: Vertex, g: Graph): List[List[Vertex]] = {
      def BFS0(elems: List[Vertex],visited: List[List[Vertex]]): List[List[Vertex]] = {
        val newNeighbors = elems.flatMap(g(_)).filterNot(visited.flatten.contains).distinct
        if (newNeighbors.isEmpty)
          visited
        else
          BFS0(newNeighbors, newNeighbors :: visited)
      }
      BFS0(List(start),List(List(start))).reverse
    }
    val result = BFS(1, g )
    println(result.mkString(","))
  }
}