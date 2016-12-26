package vlad


import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object TestScala {
  def main(args: Array[String]): Unit = {
    graphTriplets();
  }

  def graphTriplets(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Simple graph");
    val spark = new SparkContext(conf);

    val users: RDD[(VertexId, (String, String))] =
      spark.parallelize(Array(
        (3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      spark.parallelize(Array(
        Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "pi")))
    // Define a default user in case there are  relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)


    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))
    println("KKKKKKKKKKKKKKKKK");

    graph.collectNeighbors(EdgeDirection.Either).foreach(t => println(t._1 + " " + t._2.foreach(print(_))));
    println("KKKKKKKKKKKKKKKKK");
    graph.collectNeighborIds(EdgeDirection.In).foreach(t => println(t._1 + " sosedi " + t._2.foreach(t => println("Kek " + t.toString))));

  }

  def simpleGraph(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Simple graph");
    val spark = new SparkContext(conf);

    val users: RDD[(VertexId, (String, String))] =
      spark.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      spark.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    val verticesCount = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    // Print the result
    println("Vertices postdoc count: " + verticesCount);
  }

  def simpleTriangleGraph(): Unit = {
    // Creates a SparkSession.

    val conf = new SparkConf().setAppName("Simple graph");

    val spark = new SparkContext(conf);

    val sparkContextPath = "d:/spark-2.0.2-bin-hadoop2.7/data/graphx/";

    // Load the edges in canonical order and partition the graph for triangle count
    val graph = GraphLoader.edgeListFile(spark, sparkContextPath + "followers.txt", true)
      .partitionBy(PartitionStrategy.RandomVertexCut)
    // Find the triangle count for each vertex
    val triCounts = graph.triangleCount().vertices
    // Join the triangle counts with the usernames
    val users = spark.textFile(sparkContextPath + "users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
      (username, tc)
    }
    // Print the result
    println(triCountByUsername.collect().mkString("\n"))
    // $example off$
    spark.stop()
  }
}