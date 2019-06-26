import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}

//class Graph[VD, ED] {
//  val vertices: VertexRDD[VD];
//  val edges: EdgeRDD[ED]
//}


object SparkGraph {

  val threshold = 3

  /*----------------------------------------------------------------------------*/
  //                           Main 
  /*----------------------------------------------------------------------------*/
  def main(args: Array[String]) {

    //-- Enable WARN --//
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //-- Retrieve command line parameters --//
    // val threshold = args(1).toInt
    // val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))

    val inputFile = args(0)
    val outputDir = args(1)
    println("Input File: " + inputFile)
    println("Output Dir: " + outputDir)

    //social_graph(inputFile, outputDir)
    //student_graph(inputFile, outputDir)
    users_graph(inputFile, outputDir)

  }

  /*----------------------------------------------------------------------------*/
  //                          Relationship Graph 
  /*----------------------------------------------------------------------------*/
  def student_graph(inputFile: String, outputDir: String) {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Graph"))

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")),  (7L, ("jgonzal", "postdoc")),
                           (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
                           (4L, ("peter", "student")), (8L, ("kevin", "student"))
                          )
                    )


    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
        sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                             Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
                             Edge(4L, 0L, "student"),   Edge(5L, 0L, "colleague"),
                             Edge(8L, 2L, "student")
                            )
                      )

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    // print vertices
    graph.vertices.foreach(println)

    // counts students
    val students = graph.vertices.filter { case (id, (name, pos)) => pos == "student" }.count
    println("Number of students: " + students)

  }


  /*----------------------------------------------------------------------------*/
  //                         Social Graph 
  /*----------------------------------------------------------------------------*/
  def social_graph(inputFile: String, outputDir: String) {

    case class User(name: String, age: Int)

    val sc = new SparkContext(new SparkConf().setAppName("Spark Graph"))

    println("---------------------------------------------")
    println("             User Graph");
    println("---------------------------------------------")
    val users = List((1L, User("Alex", 26)),  (2L, User("Bill", 42)),  (3L, User("Carol", 18)),
                     (4L, User("Dave", 16)),  (5L, User("Eve", 45)),   (6L, User("Farell", 30)),
                     (7L, User("Garry", 32)), (8L, User("Harry", 36)), (9L, User("Ivan", 28)),
                     (10L, User("Jill", 48)))

    val usersRDD = sc.parallelize(users)
    usersRDD.foreach(println)

    println("---------------------------------------------")
    println("             Graph Edges");
    println("---------------------------------------------")
    val follows = List(Edge(1L, 2L, 1),  Edge(2L, 3L, 1),  Edge(3L, 1L, 1), Edge(3L, 4L, 1),
                       Edge(3L, 5L, 1),  Edge(4L, 5L, 1),  Edge(6L, 5L, 1), Edge(7L, 6L, 1),
                       Edge(6L, 8L, 1),  Edge(7L, 8L, 1),  Edge(7L, 9L, 1), Edge(9L, 8L, 1),
                       Edge(8L, 10L, 1), Edge(10L, 9L, 1), Edge(1L, 11L, 1))

    val followsRDD =  sc.parallelize(follows)
    followsRDD.foreach(println)

    val defaultUser = User("Alex", 1)
    val socialGraph = Graph(usersRDD, followsRDD, defaultUser)

    println("---------------------------------------------")

    val numEdges = socialGraph.numEdges
    println("Number of Edges: " + numEdges)

    println("---------------------------------------------")

    val numVertices = socialGraph.numVertices
    println("Number of Vertices: " + numVertices)

  }

  /*----------------------------------------------------------------------------*/
  //                         Social Graph 
  /*----------------------------------------------------------------------------*/
  def users_graph(inputFile: String, outputDir: String) {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Graph"))

    val vertexArray = Array(
      (1L, ("Alison" ,  3)),
      (2L, ("Bob"    , 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David"  , 42)),
      (5L, ("Edward" , 55)),
      (6L, ("Franc"  , 50)),
      (7L, ("Kevin"  , 35))
      )
    
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3),
      Edge(7L, 1L, 3)
      )

    

    // Build a Graph
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    
    println("--------------------------------------------------------")
    println("             LIKES - has least one edge                 ")
    println("--------------------------------------------------------")

    //likes
    for (triplet <- graph.triplets.collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }

    println("--------------------------------------------------------")
    println("            LOVES - has 5 or more edges                 ")
    println("--------------------------------------------------------")

    //loves
    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
      println(s"${triplet.srcAttr._1} loves ${triplet.dstAttr._1}")
    }


  }
}


