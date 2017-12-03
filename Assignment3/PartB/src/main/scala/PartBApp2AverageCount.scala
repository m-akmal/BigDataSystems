// $example on$
//import org.apache.spark.graphx.GraphLoader
// $example off$
//import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import java.lang.Object
import scala.collection.mutable.ArrayBuffer


object PartBApp2AverageCount {
  def main(args: Array[String]) {

    val file_path: String = {
      if (args.length > 0) {
        args(0)
      }
      else {
        println("No such input file found")
        "vertex_info.txt"
      }
    }

    // Creates a SparkSession.
    val conf = new SparkConf().setAppName("PartBApp2WordComapre")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.eventLog.enabled","true")
    conf.set("spark.eventLog.dir", "hdfs://10.254.0.146/spark/history")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.task.cpus", "1")
    val sc = new SparkContext(conf)
    val vfile = sc.textFile(file_path,10)
    val array = vfile.collect.toList
    println(array)

    var i = 0
    var edgeString = ""
    var edgesList = ArrayBuffer[Edge[VertexId]]()
    for ( l1<- array ){
      var j = 0
      for ( l2<-array){
        if(i==j){
          //do nothing
        } else {
          val tokens1_temp = l1.split("\\s+")
          val tokens1 = tokens1_temp.slice(1, tokens1_temp.length)
          val tokens2_temp = l2.split("\\s+")
          val tokens2 = tokens2_temp.slice(1, tokens2_temp.length)
          val intersection = tokens1.intersect(tokens2)

          if(intersection.length!=0) {
            if(tokens1.length>tokens2.length)
            {
              println(i)
              println(j)
              println("===GREATER===")
              println(intersection.mkString(" "))
              edgesList += Edge(i, j, 1L)
            }
            else
            {
              println(intersection.mkString(" "))
              edgesList += Edge(i, j, 2L)
            }
          }
        }
        j=j+1

      }
      i=i+1
    }

    val edgesRDDFromList = sc.parallelize(edgeString.split("\n"))
    val vertices: RDD[(VertexId, Array[String])] = vfile.map(line => line.split("\\s+")).zipWithIndex().map(line => (line._2, line._1))
    val edges: RDD[Edge[VertexId]] = sc.parallelize(edgesList)
    val g: Graph[Array[String], VertexId] = Graph(vertices, edges)

    def average(a: (VertexId, (Int, Int))): (VertexId, Float) = {
      if (a._2._1 == 0){
        return (a._1, -1.toFloat)
      }
      val avg = a._2._2.toFloat/a._2._1
      return (a._1, avg)
    }
    val popularVertices: VertexRDD[(Int, Int)] = g.aggregateMessages[(Int, Int)](
      triplet => { // Map Function
        triplet.sendToSrc(1, triplet.dstAttr.length)
      },
      (a, b) => (a._1+b._1, a._2 + b._2) // Reduce Function
    )
    val popularVertex: RDD[(VertexId, Float)] = popularVertices.map(vertex=>average(vertex))
    val popularVertexPrintRDD = popularVertex.map(vertex => "****Average Number of Words in Neighborhood of Vertex " + vertex._1 + " is " + vertex._2)
    popularVertexPrintRDD.collect.foreach(println(_))

    sc.stop()
    }
}

// scalastyle:on println
