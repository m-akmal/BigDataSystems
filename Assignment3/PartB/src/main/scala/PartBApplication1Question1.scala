import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{TripletFields, _}
import org.apache.spark.rdd.RDD

object PartBApplication1Question1 {
  def main(args: Array[String]) {

    val file_path: String = {
      if (args.length > 0) {
        args(0)
      }
      else {
        "soc-LiveJournal1.txt"
      }
    }
    val num_of_iterations: Int = {
      if (args.length > 1) {
        args(1).toInt
      }
      else {
        20
      }
    }
    val num_of_partitions: Int = {
      if (args.length > 2) args(2).toInt
      else {
        50
      }
    }
 // println(file_path)
    // println(num_of_iterations)
    // println(num_of_partitions)

    val conf = new SparkConf().setAppName("PartBApplication1Question1")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.eventLog.enabled","true")
    conf.set("spark.eventLog.dir", "hdfs://10.254.0.146/spark/history")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.task.cpus", "1")

    val sc = new SparkContext(conf)

    val datafile = sc.textFile(file_path, num_of_partitions)

    val raw_data = datafile.filter(f => !f.startsWith("#"))
    // raw_data.count

    val data = raw_data.map(line => line.split("\\s+"))

    // Remove partial data and self loops
    val filtered_data = data.filter(e => (e.size == 2 && !e(0).equals(e(1))))
    // filtered_data.count

    val vertices: RDD[(VertexId, Double)] = filtered_data.map(e => e(0)).distinct.map(f => (f.toInt, 1.0))

    val edges = filtered_data.map(e => Edge(e(0).toInt, e(1).toInt, "1"))

    val graph = Graph(vertices, edges)

    val outDegrees: VertexRDD[Int] = graph.outDegrees
    var rankGraph = graph.outerJoinVertices(outDegrees) { (id, oldAttr, outDegOpt) =>
      outDegOpt match {
        case Some(outDeg) => (oldAttr, outDeg)
        case None => (oldAttr, 0) // No outDegree means zero outDegree
      }
    }

    for (_ <- 1 to num_of_iterations) {

      val result: VertexRDD[(Double)] = rankGraph.aggregateMessages[(Double)](
        triplet => { // Map Function
          triplet.sendToDst((triplet.srcAttr._1) / (triplet.srcAttr._2))
        },
        // Add counter and age
        (a, b) => (a + b), // Reduce Function,
        TripletFields.Src
      )
   rankGraph = rankGraph.outerJoinVertices(result) { (id, oldAttr, newAttr) =>
        newAttr match {
          case Some(newAttr) => (0.15 + (0.85 * newAttr), oldAttr._2)
          case None => oldAttr
        }
      }
    }

    rankGraph.triplets.count
    rankGraph.triplets.take(100).foreach(println)

    sc.stop()
  }
}